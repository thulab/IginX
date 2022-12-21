package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class HashInnerJoinLazyStream extends BinaryLazyStream {

    private final InnerJoin innerJoin;

    private final HashMap<Integer, List<Row>> streamBHashMap;

    private final Deque<Row> cache;

    private Header header;

    private boolean hasInitialized = false;

    private String joinColumnA;

    private String joinColumnB;

    public HashInnerJoinLazyStream(InnerJoin innerJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.innerJoin = innerJoin;
        this.streamBHashMap = new HashMap<>();
        this.cache = new LinkedList<>();
    }

    private void initialize() throws PhysicalException {
        Filter filter = innerJoin.getFilter();
        List<String> joinColumns = new ArrayList<>(innerJoin.getJoinColumns());
        List<Field> fieldsA = new ArrayList<>(streamA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(streamB.getHeader().getFields());
        if (innerJoin.isNaturalJoin()) {
            if (!joinColumns.isEmpty()) {
                throw new InvalidOperatorParameterException("natural inner join operator should not have using operator");
            }
            for (Field fieldA : fieldsA) {
                for (Field fieldB : fieldsB) {
                    String joinColumnA = fieldA.getName().replaceFirst(innerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName().replaceFirst(innerJoin.getPrefixB() + '.', "");
                    if (joinColumnA.equals(joinColumnB)) {
                        joinColumns.add(joinColumnA);
                    }
                }
            }
            if (joinColumns.isEmpty()) {
                throw new PhysicalException("natural join has no matching columns");
            }
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }

        Header headerA = streamA.getHeader();
        Header headerB = streamB.getHeader();

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

        while (streamB.hasNext()) {
            Row rowB = streamB.next();
            int hash = rowB.getValue(innerJoin.getPrefixB() + '.' + joinColumnB).hashCode();
            List<Row> rows = streamBHashMap.getOrDefault(hash, new ArrayList<>());
            rows.add(rowB);
            streamBHashMap.putIfAbsent(hash, rows);
        }

        if (filter != null) {  // Join condition: on
            fieldsA.addAll(fieldsB);
        } else {               // Join condition: natural or using
            for (Field fieldB : fieldsB) {
                if (!fieldB.getName().equals(innerJoin.getPrefixB() + '.' + joinColumnB)) {
                    fieldsA.add(fieldB);
                }
            }
        }
        this.header = new Header(fieldsA);

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
        int hash = rowA.getValue(innerJoin.getPrefixA() + '.' + joinColumnA).hashCode();
        if (streamBHashMap.containsKey(hash)) {
            List<Row> rowsB = streamBHashMap.get(hash);
            for (Row rowB : rowsB) {
                if (innerJoin.getFilter() != null) {
                    Row row = buildRow(rowA, rowB);
                    if (FilterUtils.validate(innerJoin.getFilter(), row)) {
                        cache.addLast(row);
                    }
                } else {
                    Row row = buildRowWithUsingColumns(rowA, rowB);
                    cache.addLast(row);
                }
            }
        }
    }

    private Row buildRow(Row rowA, Row rowB) {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();
        Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
        System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
        System.arraycopy(valuesB, 0, valuesJoin, valuesA.length, valuesB.length);
        return new Row(this.header, valuesJoin);
    }

    private Row buildRowWithUsingColumns(Row rowA, Row rowB) throws PhysicalException {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();
        Object[] valuesJoin = new Object[valuesA.length + valuesB.length - 1];
        System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
        int k = valuesA.length;
        int index = streamB.getHeader().indexOf(innerJoin.getPrefixB() + '.' + joinColumnB);
        for (int j = 0; j < valuesB.length; j++) {
            if (j != index) {
                valuesJoin[k++] = valuesB[j];
            }
        }
        return new Row(this.header, valuesJoin);
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        return cache.pollFirst();
    }
}
