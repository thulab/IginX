package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LogWriter extends ExportWriter {

    private final static Logger logger = LoggerFactory.getLogger(LogWriter.class);

    @Override
    public void write(BatchData batchData) {
        Header header = batchData.getHeader();
        logger.info(header.toString());

        List<Row> rowList = batchData.getRowList();
        rowList.forEach(row -> {
            logger.info(row.toCSVTypeString());
        });
    }
}
