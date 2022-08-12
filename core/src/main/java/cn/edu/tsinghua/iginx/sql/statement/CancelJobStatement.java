package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.CancelTransformJobReq;
import cn.edu.tsinghua.iginx.thrift.Status;

public class CancelJobStatement extends SystemStatement {

    private final long jobId;

    private final IginxWorker worker = IginxWorker.getInstance();

    public CancelJobStatement(long jobId) {
        this.statementType = StatementType.CANCEL_JOB;
        this.jobId = jobId;
    }

    @Override
    public void execute(RequestContext ctx) throws ExecutionException {
        CancelTransformJobReq req = new CancelTransformJobReq(ctx.getSessionId(), jobId);
        Status status = worker.cancelTransformJob(req);

        Result result = new Result(status);
        ctx.setResult(result);
    }
}
