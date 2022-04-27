package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterTaskStatement extends SystemStatement {

    private final String filePath;

    private final String className;

    private final IginxWorker worker = IginxWorker.getInstance();

    private final static Logger logger = LoggerFactory.getLogger(RegisterTaskStatement.class);

    public RegisterTaskStatement(String filePath, String className) {
        this.statementType = StatementType.REGISTER_TASK;
        this.filePath = filePath;
        this.className = className;
    }

    @Override
    public void execute(RequestContext ctx) throws ExecutionException {
        RegisterTaskReq req = new RegisterTaskReq(ctx.getSessionId(), filePath, className);
        Status status = worker.registerTask(req);
        ctx.setResult(new Result(status));
    }
}
