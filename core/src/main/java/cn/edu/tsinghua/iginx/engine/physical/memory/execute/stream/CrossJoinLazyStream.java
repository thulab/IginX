package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import java.util.ArrayList;
import java.util.List;

public class CrossJoinLazyStream extends BinaryLazyStream {

    private final CrossJoin crossJoin;

    private final List<Row> streamBCache;

    private Header header;

    private int curStreamBIndex = 0;

    private boolean hasInitialized = false;

    private Row nextA;

    private Row nextB;

    public CrossJoinLazyStream(CrossJoin crossJoin, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.crossJoin = crossJoin;
        this.streamBCache = new ArrayList<>();
    }

    private void initialize() throws PhysicalException {
        if (hasInitialized) {
            return;
        }
        this.header = RowUtils.constructNewHead(streamA.getHeader(), streamB.getHeader(),
            crossJoin.getPrefixA(), crossJoin.getPrefixB());
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
        if (streamA.hasNext()) {
            return true;
        } else {
            return curStreamBIndex < streamBCache.size();
        }
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
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

        Row nextRow = RowUtils.constructNewRow(header, nextA, nextB);
        nextB = null;
        return nextRow;
    }
}
