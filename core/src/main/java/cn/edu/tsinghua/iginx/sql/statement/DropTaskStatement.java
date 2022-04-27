package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.DropTaskReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropTaskStatement extends SystemStatement {

    private final String fileName;

    private final IginxWorker worker = IginxWorker.getInstance();

    private final static Logger logger = LoggerFactory.getLogger(DropTaskStatement.class);

    public DropTaskStatement(String fileName) {
        this.statementType = StatementType.DROP_TASK;
        this.fileName = fileName;
    }

    @Override
    public void execute(RequestContext ctx) throws ExecutionException {
        DropTaskReq req = new DropTaskReq(ctx.getSessionId(), fileName);
        Status status = worker.dropTask(req);
        ctx.setResult(new Result(status));
    }
}
