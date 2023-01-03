package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class SortedMergeOuterJoinLazyStream extends BinaryLazyStream {

    private final OuterJoin outerJoin;

    private Header header;

    private int index;

    private boolean hasInitialized = false;

    private String joinColumnA;

    private String joinColumnB;

    private DataType joinColumnDataType;

    private Row nextA;

    private Row nextB;

    private Object curJoinColumnBValue;  // 当前StreamB中join列的值，用于同值join

    private final List<Row> sameValueStreamBRows;  // StreamB中join列的值相同的列缓存

    private final Deque<Row> cache;

    private boolean curRowsBHasMatched = false;

    private boolean lastPartHasInitialized = false;  // 外连接未匹配部分是否被初始化

    private final List<Row> unmatchedStreamARows;  // 未被匹配过的StreamA的行

    private final List<Row> unmatchedStreamBRows;  // 未被匹配过的StreamB的行

    public SortedMergeOuterJoinLazyStream(OuterJoin outerJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.outerJoin = outerJoin;
        this.sameValueStreamBRows = new ArrayList<>();
        this.cache = new LinkedList<>();
        this.unmatchedStreamARows = new ArrayList<>();
        this.unmatchedStreamBRows = new ArrayList<>();
    }

    private void initialize() throws PhysicalException {
        Filter filter = outerJoin.getFilter();

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
                throw new InvalidOperatorParameterException("sorted merge join only support one path filter yet.");
            }
            Pair<String, String> p = FilterUtils.getJoinColumnFromPathFilter((PathFilter) filter);
            if (p == null) {
                throw new InvalidOperatorParameterException("sorted merge join only support equal path filter yet.");
            }
            if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                this.joinColumnA = p.k.replaceFirst(outerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.v.replaceFirst(outerJoin.getPrefixB() + ".", "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                this.joinColumnA = p.v.replaceFirst(outerJoin.getPrefixA() + '.', "");
                this.joinColumnB = p.k.replaceFirst(outerJoin.getPrefixB() + ".", "");
            } else {
                throw new InvalidOperatorParameterException("invalid sorted merge join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException("sorted merge join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(outerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1 && headerB.indexOf(outerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                this.joinColumnA = this.joinColumnB = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid sorted merge join column input.");
            }
        }

        if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
            this.index = headerA.indexOf(outerJoin.getPrefixA() + '.' + joinColumnA);
        } else {
            this.index = headerB.indexOf(outerJoin.getPrefixB() + '.' + joinColumnB);
        }

        DataType dataTypeA = headerA.getField(headerA.indexOf(outerJoin.getPrefixA() + "." + joinColumnA)).getType();
        DataType dataTypeB = headerA.getField(headerA.indexOf(outerJoin.getPrefixA() + "." + joinColumnA)).getType();
        if (!dataTypeA.equals(dataTypeB)) {
            throw new InvalidOperatorParameterException("the datatype of join columns is different");
        }
        joinColumnDataType = dataTypeA;

        if (filter != null) {  // Join condition: on
            this.header = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());
        } else {               // Join condition: natural or using
            if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
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

        while (nextA != null) {
            unmatchedStreamARows.add(nextA);
            if (streamA.hasNext()) {
                nextA = streamA.next();
            } else {
                nextA = null;
            }
        }

        if (!curRowsBHasMatched && !sameValueStreamBRows.isEmpty()) {
            unmatchedStreamBRows.addAll(sameValueStreamBRows);
        }
        while (nextB != null) {
            unmatchedStreamBRows.add(nextB);
            if (streamB.hasNext()) {
                nextB = streamB.next();
            } else {
                nextB = null;
            }
        }

        OuterJoinType outerType = outerJoin.getOuterJoinType();
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
            int anotherRowSize = streamB.getHeader().hasTimestamp() ? streamB.getHeader().getFieldSize() + 1 : streamB.getHeader().getFieldSize();
            if (outerJoin.getFilter() == null) {
                anotherRowSize -= 1;
            }
            for (Row halfRow : unmatchedStreamARows) {
                Row unmatchedRow = RowUtils.constructUnmatchedRow(header, halfRow, anotherRowSize, true);
                cache.add(unmatchedRow);
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = streamA.getHeader().hasTimestamp() ? streamA.getHeader().getFieldSize() + 1 : streamA.getHeader().getFieldSize();
            if (outerJoin.getFilter() == null) {
                anotherRowSize -= 1;
            }
            for (Row halfRow : unmatchedStreamBRows) {
                Row unmatchedRow = RowUtils.constructUnmatchedRow(header, halfRow, anotherRowSize, false);
                cache.add(unmatchedRow);
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
        while (cache.isEmpty() && hasMoreRows()) {
            tryMatch();
        }
        if (cache.isEmpty() && !lastPartHasInitialized) {
            initializeLastPart();
        }
        return !cache.isEmpty();
    }

    private void tryMatch() throws PhysicalException {
        Object curJoinColumnAValue = nextA.getValue(outerJoin.getPrefixA() + "." + joinColumnA);
        int cmp = RowUtils.compareObjects(joinColumnDataType, curJoinColumnAValue, curJoinColumnBValue);
        if (cmp < 0) {
            unmatchedStreamARows.add(nextA);
            nextA = null;
        } else if (cmp > 0) {
            if (!curRowsBHasMatched) {
                unmatchedStreamBRows.addAll(sameValueStreamBRows);
            } else {
                curRowsBHasMatched = false;
            }
            sameValueStreamBRows.clear();
        } else {
            for (Row rowB : sameValueStreamBRows) {
                if (outerJoin.getFilter() != null) {
                    Row row = RowUtils.constructNewRow(header, nextA, rowB);
                    if (FilterUtils.validate(outerJoin.getFilter(), row)) {
                        cache.addLast(row);
                    }
                } else {
                    Row row;
                    if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
                        row = RowUtils.constructNewRow(header, nextA, rowB, new int[]{index}, false);
                    } else {
                        row = RowUtils.constructNewRow(header, nextA, rowB, new int[]{index}, true);
                    }
                    cache.addLast(row);
                }
            }
            curRowsBHasMatched = true;
            nextA = null;
        }
    }

    private boolean hasMoreRows() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        while (nextA == null && streamA.hasNext()) {
            nextA = streamA.next();
        }
        if (nextA == null) {
            return false;
        }

        while (nextB == null && streamB.hasNext()) {
            nextB = streamB.next();
        }
        while (sameValueStreamBRows.isEmpty() && nextB != null) {
            sameValueStreamBRows.add(nextB);
            curJoinColumnBValue = nextB.getValue(outerJoin.getPrefixB() + "." + joinColumnB);
            nextB = null;

            while (streamB.hasNext()) {
                nextB = streamB.next();
                Object joinColumnBValue = nextB.getValue(outerJoin.getPrefixB() + "." + joinColumnB);
                if (Objects.equals(joinColumnBValue, curJoinColumnBValue)) {
                    sameValueStreamBRows.add(nextB);
                    nextB = null;
                } else {
                    break;
                }
            }
        }
        return !sameValueStreamBRows.isEmpty();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        return cache.pollFirst();
    }
}
