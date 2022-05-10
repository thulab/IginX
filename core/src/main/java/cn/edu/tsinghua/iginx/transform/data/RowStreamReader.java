package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowStreamReader implements Reader {

    private final RowStream rowStream;

    private final int batchSize;

    private final static Logger logger = LoggerFactory.getLogger(RowStreamReader.class);

    public RowStreamReader(RowStream rowStream, int batchSize) {
        this.rowStream = rowStream;
        this.batchSize = batchSize;
    }

    @Override
    public boolean hasNextBatch() {
        try {
            return rowStream.hasNext();
        } catch (PhysicalException e) {
            logger.error("Fail to examine whether there is more data, because ", e);
            return false;
        }
    }

    @Override
    public BatchData loadNextBatch() {
        try {
            BatchData batchData = new BatchData(rowStream.getHeader());
            int countDown = batchSize;
            while (countDown > 0 && rowStream.hasNext()) {
                Row row = rowStream.next();
                batchData.appendRow(row);
                countDown--;
            }
            return batchData;
        } catch (PhysicalException e) {
            logger.error("Fail to load next batch of data, because ", e);
            return null;
        }
    }

    @Override
    public void close() {
        try {
            rowStream.close();
        } catch (PhysicalException e) {
            logger.error("Fail to close RowStream, because ", e);
        }
    }
}
