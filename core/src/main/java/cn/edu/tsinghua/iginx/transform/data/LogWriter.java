package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LogWriter extends ExportWriter {

    private boolean hasWriteHeader;

    private final static Logger logger = LoggerFactory.getLogger(LogWriter.class);

    @Override
    public void write(BatchData batchData) {
        if (!hasWriteHeader) {
            Header header = batchData.getHeader();
            List<String> headerList = new ArrayList<>();
            if (header.hasKey()) {
                headerList.add(GlobalConstant.KEY_NAME);
            }
            header.getFields().forEach(field -> headerList.add(field.getFullName()));
            logger.info(String.join(",", headerList));
            hasWriteHeader = true;
        }

        List<Row> rowList = batchData.getRowList();
        rowList.forEach(row -> {
            logger.info(row.toCSVTypeString());
        });
    }
}
