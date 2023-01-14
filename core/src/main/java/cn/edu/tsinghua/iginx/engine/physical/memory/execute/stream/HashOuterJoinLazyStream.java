package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class HashOuterJoinLazyStream extends BinaryLazyStream {

    private final OuterJoin outerJoin;

    private final HashMap<Integer, List<Row>> streamBHashMap;

    private final List<Integer> streamBHashPutOrder;

    private final List<Row> unmatchedStreamARows;  // 未被匹配过的StreamA的行

    private final Set<Integer> matchedStreamBRowHashSet;  // 已被匹配过的StreamB的行

    private final Deque<Row> cache;

    private Header header;

    private int index;

    private boolean hasInitialized = false;

    private boolean lastPartHasInitialized = false;  // 外连接未匹配部分是否被初始化

    private String joinColumnA;

    private String joinColumnB;
    
    private boolean needTypeCast = false;

    public HashOuterJoinLazyStream(OuterJoin outerJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.outerJoin = outerJoin;
        this.streamBHashMap = new HashMap<>();
        this.streamBHashPutOrder = new LinkedList<>();
        this.unmatchedStreamARows = new ArrayList<>();
        this.matchedStreamBRowHashSet = new HashSet<>();
        this.cache = new LinkedList<>();
    }

    private void initialize() throws PhysicalException {
        Filter filter = outerJoin.getFilter();
        OuterJoinType outerJoinType = outerJoin.getOuterJoinType();

        Header headerA = streamA.getHeader();
        Header headerB = streamB.getHeader();

        List<String> joinColumns = new ArrayList<>(outerJoin.getJoinColumns());
        if (outerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, headerA, headerB,
                outerJoin.getPrefixA(), outerJoin.getPrefixB());
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
                this.joinColumnA = p.k.replaceFirst(outerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.v.replaceFirst(outerJoin.getPrefixB() + ".", "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                this.joinColumnA = p.v.replaceFirst(outerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.k.replaceFirst(outerJoin.getPrefixB() + ".", "");
            } else {
                throw new InvalidOperatorParameterException("invalid hash join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException("hash join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(outerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1 && headerB.indexOf(outerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                this.joinColumnA = this.joinColumnB = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid hash join column input.");
            }
        }

        int indexAnother;
        if (outerJoinType == OuterJoinType.RIGHT) {
            this.index = headerA.indexOf(outerJoin.getPrefixA() + '.' + joinColumnA);
            indexAnother = headerB.indexOf(outerJoin.getPrefixB() + '.' + joinColumnB);
        } else {
            this.index = headerB.indexOf(outerJoin.getPrefixB() + '.' + joinColumnB);
            indexAnother = headerA.indexOf(outerJoin.getPrefixA() + '.' + joinColumnA);
        }

        DataType dataType1 = headerA.getField(indexAnother).getType();
        DataType dataType2 = headerB.getField(index).getType();
        if (ValueUtils.isNumericType(dataType1) && ValueUtils.isNumericType(dataType2)) {
            this.needTypeCast = true;
        }

        while (streamB.hasNext()) {
            Row rowB = streamB.next();
            Value value = rowB.getAsValue(outerJoin.getPrefixB() + '.' + joinColumnB);
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
            if (rows.size() == 1) {
                streamBHashPutOrder.add(hash);
            }
        }

        if (filter != null) {  // Join condition: on
            this.header = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());
        } else {               // Join condition: natural or using
            if (outerJoinType == OuterJoinType.RIGHT) {
                this.header = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), Collections.singletonList(joinColumnA), false).getV();
            } else {
                this.header = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), Collections.singletonList(joinColumnB), true).getV();
            }
        }

        this.hasInitialized = true;
    }

    private void initializeLastPart() throws PhysicalException {
        if (lastPartHasInitialized) {
            return;
        }
        OuterJoinType outerType = outerJoin.getOuterJoinType();
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
            int anotherRowSize = streamB.getHeader().hasKey() ? streamB.getHeader().getFieldSize() + 1 : streamB.getHeader().getFieldSize();
            if (outerJoin.getFilter() == null) {
                anotherRowSize -= 1;
            }
            for (Row halfRow : unmatchedStreamARows) {
                Row unmatchedRow = RowUtils.constructUnmatchedRow(header, halfRow, anotherRowSize, true);
                cache.add(unmatchedRow);
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = streamA.getHeader().hasKey() ? streamA.getHeader().getFieldSize() + 1 : streamA.getHeader().getFieldSize();
            if (outerJoin.getFilter() == null) {
                anotherRowSize -= 1;
            }
            for (int hash : streamBHashPutOrder) {
                if (!matchedStreamBRowHashSet.contains(hash)) {
                    List<Row> unmatchedRows = streamBHashMap.get(hash);
                    for (Row halfRow : unmatchedRows) {
                        Row unmatchedRow = RowUtils.constructUnmatchedRow(header, halfRow, anotherRowSize, false);
                        cache.add(unmatchedRow);
                    }
                }
            }
        }
        this.lastPartHasInitialized = true;
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
        if (cache.isEmpty() && !lastPartHasInitialized) {
            initializeLastPart();
        }
        return !cache.isEmpty();
    }

    private void tryMatch() throws PhysicalException {
        Row rowA = streamA.next();

        Value value = rowA.getAsValue(outerJoin.getPrefixA() + '.' + joinColumnA);
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
                if (outerJoin.getFilter() != null) {
                    Row row = RowUtils.constructNewRow(header, rowA, rowB);
                    if (FilterUtils.validate(outerJoin.getFilter(), row)) {
                        cache.addLast(row);
                    }
                } else {
                    Row row;
                    if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
                        row = RowUtils.constructNewRow(header, rowA, rowB, new int[]{index}, false);
                    } else {
                        row = RowUtils.constructNewRow(header, rowA, rowB, new int[]{index}, true);
                    }
                    cache.addLast(row);
                }
            }
            matchedStreamBRowHashSet.add(hash);
        } else {
            unmatchedStreamARows.add(rowA);
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
