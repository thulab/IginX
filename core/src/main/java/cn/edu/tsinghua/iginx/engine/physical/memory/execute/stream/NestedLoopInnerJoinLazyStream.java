package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;

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

        Header headerA = streamA.getHeader();
        Header headerB = streamB.getHeader();

        joinColumns = new ArrayList<>(innerJoin.getJoinColumns());
        if (innerJoin.isNaturalJoin()) {
            RowUtils.fillNaturalJoinColumns(joinColumns, headerA, headerB,
                innerJoin.getPrefixA(), innerJoin.getPrefixB());
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }

        if (filter != null) {  // Join condition: on
            this.header = RowUtils.constructNewHead(headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());
        } else {               // Join condition: natural or using
            Pair<int[], Header> pair = RowUtils.constructNewHead(headerA, headerB,
                innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumns, true);
            this.indexOfJoinColumnInTableB = pair.getK();
            this.header = pair.getV();
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
            Row row = RowUtils.constructNewRow(header, nextA, nextB);
            nextB = null;
            if (FilterUtils.validate(innerJoin.getFilter(), row)) {
                return row;
            } else {
                return null;
            }
        } else {                              // Join condition: natural or using
            for (String joinColumn : joinColumns) {
                if (ValueUtils.compare(nextA.getAsValue(innerJoin.getPrefixA() + '.' + joinColumn),
                        nextB.getAsValue(innerJoin.getPrefixB() + '.' + joinColumn)) != 0) {
                    nextB = null;
                    return null;
                }
            }
            Row row = RowUtils.constructNewRow(header, nextA, nextB, indexOfJoinColumnInTableB, true);
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
