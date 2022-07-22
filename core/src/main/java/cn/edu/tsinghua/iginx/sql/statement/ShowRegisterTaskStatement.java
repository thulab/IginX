package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.GetRegisterTaskInfoReq;
import cn.edu.tsinghua.iginx.thrift.GetRegisterTaskInfoResp;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskInfo;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

import java.util.List;

public class ShowRegisterTaskStatement extends SystemStatement {

    private final IginxWorker worker = IginxWorker.getInstance();

    public ShowRegisterTaskStatement() {
        this.statementType = StatementType.SHOW_REGISTER_TASK;
    }

    @Override
    public void execute(RequestContext ctx) throws ExecutionException {
        GetRegisterTaskInfoReq req = new GetRegisterTaskInfoReq(ctx.getSessionId());
        GetRegisterTaskInfoResp resp = worker.getRegisterTaskInfo(req);
        List<RegisterTaskInfo> taskInfos = resp.getRegisterTaskInfoList();

        Result result = new Result(RpcUtils.SUCCESS);
        result.setRegisterTaskInfos(taskInfos);
        ctx.setResult(result);
    }
}
