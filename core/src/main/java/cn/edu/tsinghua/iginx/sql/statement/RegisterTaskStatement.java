package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterTaskStatement extends SystemStatement {

    private final String name;

    private final String filePath;

    private final String className;

    private final UDFType type;

    private final IginxWorker worker = IginxWorker.getInstance();

    private final static Logger logger = LoggerFactory.getLogger(RegisterTaskStatement.class);

    public RegisterTaskStatement(String name, String filePath, String className, UDFType type) {
        this.statementType = StatementType.REGISTER_TASK;
        this.name = name;
        this.filePath = filePath;
        this.className = className;
        this.type = type;
    }

    @Override
    public void execute(RequestContext ctx) throws ExecutionException {
        RegisterTaskReq req = new RegisterTaskReq(ctx.getSessionId(), name, filePath, className, type);
        Status status = worker.registerTask(req);
        ctx.setResult(new Result(status));
    }
}
