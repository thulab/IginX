package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NestedLoopOuterJoinLazyStream extends BinaryLazyStream {

    private final OuterJoin outerJoin;

    private final List<Row> streamBCache;

    private final List<Row> unmatchedStreamARows;  // 未被匹配过的StreamA的行

    private final Set<Integer> matchedStreamBRowIndexSet;  // 已被匹配过的StreamB的行

    private final List<Row> lastPart;  // 后面多出未被匹配的结果行

    private List<String> joinColumns;

    private int[] indexOfJoinColumnsInTable;

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

        Header headerA = streamA.getHeader();
        Header headerB = streamB.getHeader();

        joinColumns = new ArrayList<>(outerJoin.getJoinColumns());
        if (outerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, headerA, headerB,
                outerJoin.getPrefixA(), outerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }

        if (filter != null) {  // Join condition: on
            this.header = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());
        } else {               // Join condition: natural or using
            Pair<int[], Header> pair;
            if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
                pair = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), joinColumns, false);
            } else {
                pair = RowUtils.constructNewHead(headerA, headerB, outerJoin.getPrefixA(),
                    outerJoin.getPrefixB(), joinColumns, true);
            }
            this.indexOfJoinColumnsInTable = pair.getK();
            this.header = pair.getV();
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
                anotherRowSize -= joinColumns.size();
            }
            for (Row halfRow : unmatchedStreamARows) {
                Row unmatchedRow = RowUtils.constructUnmatchedRow(header, halfRow, anotherRowSize, true);
                lastPart.add(unmatchedRow);
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = streamA.getHeader().hasKey() ? streamA.getHeader().getFieldSize() + 1 : streamA.getHeader().getFieldSize();
            if (outerJoin.getFilter() == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < streamBCache.size(); i++) {
                if (!matchedStreamBRowIndexSet.contains(i)) {
                    Row unmatchedRow = RowUtils.constructUnmatchedRow(header, streamBCache.get(i), anotherRowSize, false);
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
            Row row = RowUtils.constructNewRow(header, nextA, nextB);
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
                if (ValueUtils.compare(nextA.getAsValue(outerJoin.getPrefixA() + '.' + joinColumn),
                        nextB.getAsValue(outerJoin.getPrefixB() + '.' + joinColumn)) != 0) {
                    nextB = null;
                    return null;
                }
            }
            // matched
            this.curNextAHasMatched = true;
            this.matchedStreamBRowIndexSet.add(curStreamBIndex - 1);
            Row row;
            if (outerJoin.getOuterJoinType() == OuterJoinType.RIGHT) {
                row = RowUtils.constructNewRow(header, nextA, nextB, indexOfJoinColumnsInTable, false);
            } else {
                row = RowUtils.constructNewRow(header, nextA, nextB, indexOfJoinColumnsInTable, true);
            }
            nextB = null;
            return row;
        }
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
