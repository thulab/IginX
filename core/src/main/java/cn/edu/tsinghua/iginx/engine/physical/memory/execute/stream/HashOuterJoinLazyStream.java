package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
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

    private List<String> joinColumns;

    private Header header;

    private boolean hasInitialized = false;

    private boolean lastPartHasInitialized = false;  // 外连接未匹配部分是否被初始化

    private String joinColumnA;

    private String joinColumnB;

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
        joinColumns = new ArrayList<>(outerJoin.getJoinColumns());
        List<Field> fieldsA = new ArrayList<>(streamA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(streamB.getHeader().getFields());
        if (outerJoin.isNaturalJoin()) {
            if (!joinColumns.isEmpty()) {
                throw new InvalidOperatorParameterException("natural inner join operator should not have using operator");
            }
            for (Field fieldA : fieldsA) {
                for (Field fieldB : fieldsB) {
                    String joinColumnA = fieldA.getName().replaceFirst(outerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName().replaceFirst(outerJoin.getPrefixB() + '.', "");
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

        while (streamB.hasNext()) {
            Row rowB = streamB.next();
            int hash = rowB.getValue(outerJoin.getPrefixB() + '.' + joinColumnB).hashCode();
            List<Row> rows = streamBHashMap.getOrDefault(hash, new ArrayList<>());
            rows.add(rowB);
            streamBHashMap.putIfAbsent(hash, rows);
            if (rows.size() == 1) {
                streamBHashPutOrder.add(hash);
            }
        }

        List<Field> newFields = new ArrayList<>();
        if (filter != null) {  // Join condition: on
            newFields.addAll(fieldsA);
            newFields.addAll(fieldsB);
        } else {               // Join condition: natural or using
            OuterJoinType outerJoinType = outerJoin.getOuterJoinType();
            if (outerJoinType == OuterJoinType.RIGHT) {
                for (Field fieldA : fieldsA) {
                    if (!fieldA.getName().equals(outerJoin.getPrefixA() + '.' + joinColumnA)) {
                        newFields.add(fieldA);
                    }
                }
                newFields.addAll(fieldsB);
            } else {
                newFields.addAll(fieldsA);
                for (Field fieldB : fieldsB) {
                    if (!fieldB.getName().equals(outerJoin.getPrefixB() + '.' + joinColumnB)) {
                        newFields.add(fieldB);
                    }
                }
            }
        }
        this.header = new Header(newFields);

        this.hasInitialized = true;
    }

    private void initializeLastPart() throws PhysicalException {
        if (lastPartHasInitialized) {
            return;
        }
        OuterJoinType outerType = outerJoin.getOuterJoinType();
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
            int anotherRowSize = streamB.getHeader().getFieldSize();
            if (outerJoin.getFilter() == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (Row halfRow : unmatchedStreamARows) {
                Row unmatchedRow = buildUnmatchedRow(halfRow, anotherRowSize, true);
                cache.add(unmatchedRow);
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = streamA.getHeader().getFieldSize();
            if (outerJoin.getFilter() == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int hash : streamBHashPutOrder) {
                if (!matchedStreamBRowHashSet.contains(hash)) {
                    List<Row> unmatchedRows = streamBHashMap.get(hash);
                    for (Row halfRow : unmatchedRows) {
                        Row unmatchedRow = buildUnmatchedRow(halfRow, anotherRowSize, false);
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
        int hash = rowA.getValue(outerJoin.getPrefixA() + '.' + joinColumnA).hashCode();
        if (streamBHashMap.containsKey(hash)) {
            List<Row> rowsB = streamBHashMap.get(hash);
            for (Row rowB : rowsB) {
                if (outerJoin.getFilter() != null) {
                    Row row = buildRow(rowA, rowB);
                    if (FilterUtils.validate(outerJoin.getFilter(), row)) {
                        cache.addLast(row);
                    }
                } else {
                    Row row = buildRowWithUsingColumns(rowA, rowB);
                    cache.addLast(row);
                }
            }
            matchedStreamBRowHashSet.add(hash);
        } else {
            unmatchedStreamARows.add(rowA);
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
        OuterJoinType outerType = outerJoin.getOuterJoinType();
        if (outerType == OuterJoinType.RIGHT) {
            System.arraycopy(valuesB, 0, valuesJoin, valuesA.length - 1, valuesB.length);
            int k = 0;
            int index = streamA.getHeader().indexOf(outerJoin.getPrefixA() + '.' + joinColumnA);
            for (int j = 0; j < valuesA.length; j++) {
                if (j != index) {
                    valuesJoin[k++] = valuesA[j];
                }
            }
        } else {
            System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
            int k = valuesA.length;
            int index = streamB.getHeader().indexOf(outerJoin.getPrefixB() + '.' + joinColumnB);
            for (int j = 0; j < valuesB.length; j++) {
                if (j != index) {
                    valuesJoin[k++] = valuesB[j];
                }
            }
        }
        return new Row(this.header, valuesJoin);
    }

    private Row buildUnmatchedRow(Row halfRow, int anotherRowSize, boolean putLeft) {
        Object[] valuesJoin = new Object[halfRow.getValues().length + anotherRowSize];
        if (putLeft) {
            System.arraycopy(halfRow.getValues(), 0, valuesJoin, 0, halfRow.getValues().length);
        } else {
            System.arraycopy(halfRow.getValues(), 0, valuesJoin, anotherRowSize, halfRow.getValues().length);
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
