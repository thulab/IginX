package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.thrift.ShowEligibleJobReq;
import cn.edu.tsinghua.iginx.thrift.ShowEligibleJobResp;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

import java.util.List;

public class ShowEligibleJobStatement extends SystemStatement {

    private final JobState jobState;

    private final IginxWorker worker = IginxWorker.getInstance();

    public ShowEligibleJobStatement(JobState jobState) {
        this.statementType = StatementType.SHOW_ELIGIBLE_JOB;
        this.jobState = jobState;
    }

    @Override
    public void execute(RequestContext ctx) throws ExecutionException {
        ShowEligibleJobReq req = new ShowEligibleJobReq(ctx.getSessionId(), jobState);
        ShowEligibleJobResp resp = worker.showEligibleJob(req);
        List<Long> jobIdList = resp.getJobIdList();

        Result result = new Result(RpcUtils.SUCCESS);
        result.setJobIdList(jobIdList);
        ctx.setResult(result);
    }
}
