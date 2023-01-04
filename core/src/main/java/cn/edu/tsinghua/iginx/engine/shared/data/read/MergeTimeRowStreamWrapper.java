package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import java.util.ArrayList;
import java.util.List;

public class MergeTimeRowStreamWrapper implements RowStream {

    private final RowStream rowStream;

    private Row nextRow;

    private Row lookAhead;

    public MergeTimeRowStreamWrapper(RowStream rowStream) {
        this.rowStream = rowStream;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        return rowStream.getHeader();
    }

    @Override
    public void close() throws PhysicalException {
        rowStream.close();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (nextRow != null) {
            return true;
        }
        loadNextRow();
        return nextRow != null;
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new PhysicalException("the row stream has used up");
        }
        Row row = nextRow;
        nextRow = null;
        return row;
    }

    private void loadNextRow() throws PhysicalException {
        if (nextRow != null) {
            return;
        }

        List<Row> sameTimeRows = new ArrayList<>();
        long currentTime = -1;
        if (lookAhead != null) {
            currentTime = lookAhead.getKey();
            sameTimeRows.add(lookAhead);
            lookAhead = null;
        }
        while (rowStream.hasNext()) {
            lookAhead = rowStream.next();
            if (currentTime == -1) {
                currentTime = lookAhead.getKey();
                sameTimeRows.add(lookAhead);
                lookAhead = null;
            } else if (currentTime == lookAhead.getKey()) {
                sameTimeRows.add(lookAhead);
                lookAhead = null;
            } else {
                break;
            }
        }
        nextRow = mergeRows(sameTimeRows);
    }

    private Row mergeRows(List<Row> sameTimeRows) {
        if (sameTimeRows == null || sameTimeRows.isEmpty()) {
            return null;
        }

        Row ret = sameTimeRows.get(0);
        for (int i = 1; i < sameTimeRows.size(); i++) {
            ret = mergeTwoRows(ret, sameTimeRows.get(i));
        }
        return ret;
    }

    private Row mergeTwoRows(Row row1, Row row2) {
        int fieldSize = row1.getHeader().getFieldSize();
        Object[] values = new Object[fieldSize];
        for (int i = 0; i < fieldSize; i++) {
            if (row2.getValue(i) != null) {
                values[i] = row2.getValue(i);
            } else {
                values[i] = row1.getValue(i);
            }
        }
        return new Row(row1.getHeader(), row1.getKey(), values);
    }
}
