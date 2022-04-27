package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.transform.api.Reader;

import java.util.List;

public class SplitReader implements Reader {

    private final List<Row> rowList;

    private final Header header;

    private final int batchSize;

    private int offset = 0;

    public SplitReader(BatchData batchData, int batchSize) {
        this.rowList = batchData.getRowList();
        this.header = batchData.getHeader();
        this.batchSize = batchSize;
    }

    @Override
    public boolean hasNextBatch() {
        return offset < rowList.size();
    }

    @Override
    public BatchData loadNextBatch() {
        BatchData batchData = new BatchData(header);
        int countDown = batchSize;
        while (countDown > 0 && offset < rowList.size()) {
            batchData.appendRow(rowList.get(offset));
            countDown--;
            offset++;
        }
        return batchData;
    }

    @Override
    public void close() {
        rowList.clear();
    }
}
