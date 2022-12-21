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
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class NestedLoopOuterJoinLazyStream extends BinaryLazyStream {

    private final OuterJoin outerJoin;

    private final List<Row> streamBCache;

    private final List<Row> unmatchedStreamARows;  // 未被匹配过的StreamA的行

    private final Set<Integer> matchedStreamBRowIndexSet;  // 已被匹配过的StreamB的行

    private final List<Row> lastPart;  // 后面多出未被匹配的结果行

    private List<String> joinColumns;

    private int[] indexOfJoinColumnInTableA;

    private int[] indexOfJoinColumnInTableB;

    private Header header;

    private boolean curNextAHasMatched = false;  // 当前streamA的Row是否已被匹配过

    private int curStreamBIndex = 0;

    private boolean hasInitialized = false;

    private boolean lastPartHasInitialized = false;  // 外连接未匹配部分是否被初始化

    private int lastPartIndex = 0;

    private Row nextA;

    private Row nextB;

    private Row nextRow;

    public NestedLoopOuterJoinLazyStream(OuterJoin outerJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.outerJoin = outerJoin;
        this.streamBCache = new ArrayList<>();
        this.unmatchedStreamARows = new ArrayList<>();
        this.matchedStreamBRowIndexSet = new HashSet<>();
        this.lastPart = new ArrayList<>();
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

        if (filter != null) {  // Join condition: on
            fieldsA.addAll(fieldsB);
            this.header = new Header(fieldsA);
        } else {               // Join condition: natural or using
            List<Field> newFields = new ArrayList<>();
            this.indexOfJoinColumnInTableA = new int[joinColumns.size()];
            this.indexOfJoinColumnInTableB = new int[joinColumns.size()];
            OuterJoinType outerType = outerJoin.getOuterJoinType();
            int i = 0, j = 0;
            if (outerType == OuterJoinType.RIGHT) {
                flag1:
                for (Field fieldA : fieldsA) {
                    for (String joinColumn : joinColumns) {
                        if (Objects.equals(fieldA.getName(), outerJoin.getPrefixA() + '.' + joinColumn)) {
                            indexOfJoinColumnInTableA[j++] = streamA.getHeader().indexOf(fieldA);
                            continue flag1;
                        }
                    }
                    newFields.add(fieldA);
                }
                newFields.addAll(fieldsB);
            } else {
                newFields.addAll(fieldsA);
                flag2:
                for (Field fieldB : fieldsB) {
                    for (String joinColumn : joinColumns) {
                        if (Objects.equals(fieldB.getName(), outerJoin.getPrefixB() + '.' + joinColumn)) {
                            indexOfJoinColumnInTableB[i++] = streamB.getHeader().indexOf(fieldB);
                            continue flag2;
                        }
                    }
                    newFields.add(fieldB);
                }
            }
            this.header = new Header(newFields);
        }
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
                lastPart.add(unmatchedRow);
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = streamA.getHeader().getFieldSize();
            if (outerJoin.getFilter() == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < streamBCache.size(); i++) {
                if (!matchedStreamBRowIndexSet.contains(i)) {
                    Row unmatchedRow = buildUnmatchedRow(streamBCache.get(i), anotherRowSize, false);
                    lastPart.add(unmatchedRow);
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
        if (nextRow != null) {
            return true;
        }
        while (nextRow == null && hasMoreRows()) {
            nextRow = tryMatch();
        }
        if (nextRow == null) {
            initializeLastPart();
            if (lastPartIndex < lastPart.size()) {
                nextRow = lastPart.get(lastPartIndex);
                lastPartIndex++;
            }
        }
        return nextRow != null;
    }

    private boolean hasMoreRows() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        if (streamA.hasNext()) {
            return true;
        } else {
            if (curStreamBIndex < streamBCache.size()) {
                return true;
            } else {
                if (nextA != null && !curNextAHasMatched) {
                    unmatchedStreamARows.add(nextA);
                    nextA = null;
                }
                return false;
            }
        }
    }

    private Row tryMatch() throws PhysicalException {
        if (!hasMoreRows()) {
            return null;
        }

        if (nextA == null && streamA.hasNext()) {
            nextA = streamA.next();
        }
        if (nextB == null) {
            if (streamB.hasNext()) {
                nextB = streamB.next();
                streamBCache.add(nextB);
            } else if (curStreamBIndex < streamBCache.size()) {
                nextB = streamBCache.get(curStreamBIndex);
            } else {  // streamB和streamA中的一行全部匹配过了一遍
                if (!curNextAHasMatched) {
                    unmatchedStreamARows.add(nextA);
                }
                nextA = streamA.next();
                curNextAHasMatched = false;
                curStreamBIndex = 0;
                nextB = streamBCache.get(curStreamBIndex);
            }
            curStreamBIndex++;
        }

        if (outerJoin.getFilter() != null) {  // Join condition: on
            Row row = buildRow(nextA, nextB);
            nextB = null;
            if (FilterUtils.validate(outerJoin.getFilter(), row)) {  // matched
                this.curNextAHasMatched = true;
                this.matchedStreamBRowIndexSet.add(curStreamBIndex - 1);
                return row;
            } else {
                return null;
            }
        } else {                              // Join condition: natural or using
            for (String joinColumn : joinColumns) {
                if (!Objects.equals(nextA.getValue(outerJoin.getPrefixA() + '.' + joinColumn), nextB.getValue(outerJoin.getPrefixB() + '.' + joinColumn))) {
                    nextB = null;
                    return null;
                }
            }
            // matched
            this.curNextAHasMatched = true;
            this.matchedStreamBRowIndexSet.add(curStreamBIndex - 1);
            Row row = buildRowWithUsingColumns(nextA, nextB);
            nextB = null;
            return row;
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

    private Row buildRowWithUsingColumns(Row rowA, Row rowB) {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();
        Object[] valuesJoin;
        if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
            valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnInTableA.length];
            System.arraycopy(valuesB, 0, valuesJoin, valuesA.length - indexOfJoinColumnInTableA.length, valuesB.length);
            int k = 0;
            flag1:
            for (int m = 0; m < valuesA.length; m++) {
                for (int index : indexOfJoinColumnInTableA) {
                    if (m == index) {
                        continue flag1;
                    }
                }
                valuesJoin[k++] = valuesA[m];
            }
        } else {
            valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnInTableB.length];
            System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
            int k = valuesA.length;
            flag2:
            for (int m = 0; m < valuesB.length; m++) {
                for (int index : indexOfJoinColumnInTableB) {
                    if (m == index) {
                        continue flag2;
                    }
                }
                valuesJoin[k++] = valuesB[m];
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

        Row ret = nextRow;
        nextRow = null;
        return ret;
    }
}
