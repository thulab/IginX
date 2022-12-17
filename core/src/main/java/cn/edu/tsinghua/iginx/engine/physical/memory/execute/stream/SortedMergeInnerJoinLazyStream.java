package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/**
 * input two stream must be ascending order.
 * */
public class SortedMergeInnerJoinLazyStream extends BinaryLazyStream {

    private final InnerJoin innerJoin;

    private Header header;

    private boolean hasInitialized = false;

    private String joinColumnA;

    private String joinColumnB;

    private DataType joinColumnDataType;

    private Row nextA;

    private Row nextB;

    private Object curJoinColumnBValue;  // 当前StreamB中join列的值，用于同值join

    private final List<Row> sameValueStreamBRows;  // StreamB中join列的值相同的列缓存

    private final Deque<Row> cache;

    public SortedMergeInnerJoinLazyStream(InnerJoin innerJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.innerJoin = innerJoin;
        this.sameValueStreamBRows = new ArrayList<>();
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
                throw new InvalidOperatorParameterException("sorted merge join only support one path filter yet.");
            }
            Pair<String, String> p = FilterUtils.getJoinColumnFromPathFilter((PathFilter) filter);
            if (p == null) {
                throw new InvalidOperatorParameterException("sorted merge join only support equal path filter yet.");
            }
            if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                this.joinColumnA = p.k.replaceFirst(innerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.v.replaceFirst(innerJoin.getPrefixB() + ".", "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                this.joinColumnA = p.v.replaceFirst(innerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.k.replaceFirst(innerJoin.getPrefixB() + ".", "");
            } else {
                throw new InvalidOperatorParameterException("invalid sorted merge join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException("sorted merge join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(innerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1 && headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                this.joinColumnA = this.joinColumnB = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid sorted merge join column input.");
            }
        }

        DataType dataTypeA = headerA.getField(headerA.indexOf(innerJoin.getPrefixA() + "." + joinColumnA)).getType();
        DataType dataTypeB = headerA.getField(headerA.indexOf(innerJoin.getPrefixA() + "." + joinColumnA)).getType();
        if (!dataTypeA.equals(dataTypeB)) {
            throw new InvalidOperatorParameterException("the datatype of join columns is different");
        }
        joinColumnDataType = dataTypeA;

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
        while (cache.isEmpty() && hasMoreRows()) {
            tryMatch();
        }
        return !cache.isEmpty();
    }

    private void tryMatch() throws PhysicalException {
        Object curJoinColumnAValue = nextA.getValue(innerJoin.getPrefixA() + "." + joinColumnA);
        int cmp = RowUtils.compareObjects(joinColumnDataType, curJoinColumnAValue, curJoinColumnBValue);
        if (cmp < 0) {
            nextA = null;
        } else if (cmp > 0) {
            sameValueStreamBRows.clear();
        } else {
            for (Row rowB : sameValueStreamBRows) {
                if (innerJoin.getFilter() != null) {
                    Row row = buildRow(nextA, rowB);
                    if (FilterUtils.validate(innerJoin.getFilter(), row)) {
                        cache.addLast(row);
                    }
                } else {
                    Row row = buildRowWithUsingColumns(nextA, rowB);
                    cache.addLast(row);
                }
            }
            nextA = null;
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

    private boolean hasMoreRows() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        while (nextA == null && streamA.hasNext()) {
            nextA = streamA.next();
        }
        while (nextB == null && streamB.hasNext()) {
            nextB = streamB.next();
        }
        while (sameValueStreamBRows.isEmpty() && nextB != null) {
            sameValueStreamBRows.add(nextB);
            curJoinColumnBValue = nextB.getValue(innerJoin.getPrefixB() + "." + joinColumnB);
            nextB = null;

            while (streamB.hasNext()) {
                nextB = streamB.next();
                Object joinColumnBValue = nextB.getValue(innerJoin.getPrefixB() + "." + joinColumnB);
                if (Objects.equals(joinColumnBValue, curJoinColumnBValue)) {
                    sameValueStreamBRows.add(nextB);
                    nextB = null;
                } else {
                    break;
                }
            }
        }
        return nextA != null && !sameValueStreamBRows.isEmpty();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        return cache.pollFirst();
    }
}
