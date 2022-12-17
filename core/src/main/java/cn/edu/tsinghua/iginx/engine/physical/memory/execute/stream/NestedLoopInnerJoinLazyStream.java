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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NestedLoopInnerJoinLazyStream extends BinaryLazyStream {

    private final InnerJoin innerJoin;

    private final List<Row> streamBCache;

    private int[] indexOfJoinColumnInTableB;

    private List<String> joinColumns;

    private Header header;

    private int curStreamBIndex = 0;

    private boolean hasInitialized = false;

    private Row nextA;

    private Row nextB;

    private Row nextRow;

    public NestedLoopInnerJoinLazyStream(InnerJoin innerJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.innerJoin = innerJoin;
        this.streamBCache = new ArrayList<>();
    }

    private void initialize() throws PhysicalException {
        Filter filter = innerJoin.getFilter();
        joinColumns = new ArrayList<>(innerJoin.getJoinColumns());
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

        if (filter != null) {  // Join condition: on
            fieldsA.addAll(fieldsB);
        } else {               // Join condition: natural or using
            this.indexOfJoinColumnInTableB = new int[joinColumns.size()];
            Header headerB = streamB.getHeader();
            int index = 0;
            flag:
            for (Field fieldB : fieldsB) {
                for (String joinColumn : joinColumns) {
                    if (Objects.equals(fieldB.getName(), innerJoin.getPrefixB() + '.' + joinColumn)) {
                        indexOfJoinColumnInTableB[index++] = headerB.indexOf(fieldB);
                        continue flag;
                    }
                }
                fieldsA.add(fieldB);
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
        if (nextRow != null) {
            return true;
        }
        while (nextRow == null && hasMoreRows()) {
            nextRow = tryMatch();
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
            return curStreamBIndex < streamBCache.size();
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
                nextA = streamA.next();
                curStreamBIndex = 0;
                nextB = streamBCache.get(curStreamBIndex);
            }
            curStreamBIndex++;
        }

        if (innerJoin.getFilter() != null) {  // Join condition: on
            Row row = buildRow(nextA, nextB);
            nextB = null;
            if (FilterUtils.validate(innerJoin.getFilter(), row)) {
                return row;
            } else {
                return null;
            }
        } else {                              // Join condition: natural or using
            for (String joinColumn : joinColumns) {
                if (!Objects.equals(nextA.getValue(innerJoin.getPrefixA() + '.' + joinColumn), nextB.getValue(innerJoin.getPrefixB() + '.' + joinColumn))) {
                    nextB = null;
                    return null;
                }
            }
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
        Object[] valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnInTableB.length];
        System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
        int cur = valuesA.length;
        flag3:
        for (int i = 0; i < valuesB.length; i++) {
            for (int index : indexOfJoinColumnInTableB) {
                if (i == index) {
                    continue flag3;
                }
            }
            valuesJoin[cur++] = valuesB[i];
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
