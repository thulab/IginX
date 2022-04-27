package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;

public class CollectionWriter extends ExportWriter {

    private BatchData collectedData;

    @Override
    public void write(BatchData batchData) {
        if (collectedData == null) {
            collectedData = new BatchData(batchData.getHeader());
        }
        for (Row row : batchData.getRowList()) {
            collectedData.appendRow(row);
        }
    }

    public BatchData getCollectedData() {
        return collectedData;
    }
}
