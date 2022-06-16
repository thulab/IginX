package cn.edu.tsinghua.iginx.transform.data;

import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.ExecuteStatementReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (!batchData.getHeader().hasTimestamp()) {
            logger.error("There are no time series in the data written back.");
            return "";
        }
        builder.append("INSERT INTO transform(Time, ");
        batchData.getHeader().getFields().forEach(field -> builder.append(field.getFullName()).append(","));
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") VALUES");
        for (Row row : batchData.getRowList()) {
            builder.append(" (");
            builder.append(row.getTimestamp()).append(",");
            for (Object value : row.getValues()) {
                builder.append(value + ",");
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append("),");
        }
        builder.deleteCharAt(builder.length() - 1).append(";");
        return builder.toString();
    }
}
