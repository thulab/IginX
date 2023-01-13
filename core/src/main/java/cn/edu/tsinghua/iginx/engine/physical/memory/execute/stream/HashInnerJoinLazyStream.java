package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class HashInnerJoinLazyStream extends BinaryLazyStream {

    private final InnerJoin innerJoin;

    private final HashMap<Integer, List<Row>> streamBHashMap;

    private final Deque<Row> cache;

    private Header header;

    private int index;

    private boolean hasInitialized = false;

    private String joinColumnA;

    private String joinColumnB;
    
    private boolean needTypeCast = false;

    public HashInnerJoinLazyStream(InnerJoin innerJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.innerJoin = innerJoin;
        this.streamBHashMap = new HashMap<>();
        this.cache = new LinkedList<>();
    }

    private void initialize() throws PhysicalException {
        Filter filter = innerJoin.getFilter();

        Header headerA = streamA.getHeader();
        Header headerB = streamB.getHeader();

        List<String> joinColumns = new ArrayList<>(innerJoin.getJoinColumns());
        if (innerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, headerA, headerB,
                innerJoin.getPrefixA(), innerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }

        if (filter != null) {
            if (!filter.getType().equals(FilterType.Path)) {
                throw new InvalidOperatorParameterException("hash join only support one path filter yet.");
            }
            Pair<String, String> p = FilterUtils.getJoinColumnFromPathFilter((PathFilter) filter);
            if (p == null) {
                throw new InvalidOperatorParameterException("hash join only support equal path filter yet.");
            }
            if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                this.joinColumnA = p.k.replaceFirst(innerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.v.replaceFirst(innerJoin.getPrefixB() + ".", "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                this.joinColumnA = p.v.replaceFirst(innerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.k.replaceFirst(innerJoin.getPrefixB() + ".", "");
            } else {
                throw new InvalidOperatorParameterException("invalid hash join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException("hash join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(innerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1 && headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                this.joinColumnA = this.joinColumnB = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid hash join column input.");
            }
        }
        this.index = headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumnB);
    
        int indexA = headerA.indexOf(innerJoin.getPrefixA() + '.' + joinColumnA);
        DataType dataTypeA = headerA.getField(indexA).getType();
        DataType dataTypeB = headerB.getField(index).getType();
        if (ValueUtils.isNumericType(dataTypeA) && ValueUtils.isNumericType(dataTypeB)) {
            this.needTypeCast = true;
        }

        while (streamB.hasNext()) {
            Row rowB = streamB.next();
            Value value = rowB.getAsValue(innerJoin.getPrefixB() + '.' + joinColumnB);
            if (value == null) {
                continue;
            }
            if (needTypeCast) {
                value = ValueUtils.transformToDouble(value);
            }
            int hash;
            if (value.getDataType() == DataType.BINARY) {
                hash = Arrays.hashCode(value.getBinaryV());
            } else {
                hash = value.getValue().hashCode();
            }
            List<Row> rows = streamBHashMap.getOrDefault(hash, new ArrayList<>());
            rows.add(rowB);
            streamBHashMap.putIfAbsent(hash, rows);
        }

        if (filter != null) {  // Join condition: on
            this.header = RowUtils.constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());
        } else {               // Join condition: natural or using
            this.header = RowUtils.constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB(),
                Collections.singletonList(joinColumnB), true).getV();
        }

        this.hasInitialized = true;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        return header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        while (cache.isEmpty() && streamA.hasNext()) {
            tryMatch();
        }
        return !cache.isEmpty();
    }

    private void tryMatch() throws PhysicalException {
        Row rowA = streamA.next();

        Value value = rowA.getAsValue(innerJoin.getPrefixA() + '.' + joinColumnA);
        if (value == null) {
            return;
        }
        if (needTypeCast) {
            value = ValueUtils.transformToDouble(value);
        }
        int hash;
        if (value.getDataType() == DataType.BINARY) {
            hash = Arrays.hashCode(value.getBinaryV());
        } else {
            hash = value.getValue().hashCode();
        }

        if (streamBHashMap.containsKey(hash)) {
            List<Row> rowsB = streamBHashMap.get(hash);
            for (Row rowB : rowsB) {
                if (innerJoin.getFilter() != null) {
                    Row row = RowUtils.constructNewRow(header, rowA, rowB);
                    if (FilterUtils.validate(innerJoin.getFilter(), row)) {
                        cache.addLast(row);
                    }
                } else {
                    Row row = RowUtils.constructNewRow(header, rowA, rowB, new int[]{index}, true);
                    cache.addLast(row);
                }
            }
        }
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        return cache.pollFirst();
    }
}
