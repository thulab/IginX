package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.ExecuteStatementReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IginXWriter extends ExportWriter {

    private final long sessionId;

    private final StatementExecutor executor = StatementExecutor.getInstance();

    private final ContextBuilder contextBuilder = ContextBuilder.getInstance();

    private final static Logger logger = LoggerFactory.getLogger(IginXWriter.class);

    public IginXWriter(long sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public void write(BatchData batchData) {
        String insertSQL = buildSQL(batchData);
        logger.info("Insert statement: " + insertSQL);

        if (!insertSQL.equals("")) {
            ExecuteStatementReq req = new ExecuteStatementReq(sessionId, insertSQL);
            RequestContext context = contextBuilder.build(req);
            executor.execute(context);
        } else {
            logger.error("Fail to execute insert statement.");
        }
    }

    private String buildSQL(BatchData batchData) {
        StringBuilder builder = new StringBuilder();

        // construct paths
        builder.append("INSERT INTO transform(Time, ");
        Header header = batchData.getHeader();
        header.getFields().forEach(field ->
            builder.append(reformatPath(field.getFullName())).append(",")
        );
        builder.deleteCharAt(builder.length() - 1);

        // construct values
        builder.append(") VALUES");
        long index = System.currentTimeMillis();
        for (Row row : batchData.getRowList()) {
            builder.append(" (");
            builder.append(index).append(",");
            for (Object value : row.getValues()) {
                builder.append(value + ",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append("),");
            index++;
        }
        builder.deleteCharAt(builder.length() - 1).append(";");
        return builder.toString();
    }

    private String reformatPath(String path) {
        if (!path.contains("(") && !path.contains(")"))
            return path;
        path = path.replaceAll("[{]", "[");
        path = path.replaceAll("[}]", "]");
        return path;
    }
}
