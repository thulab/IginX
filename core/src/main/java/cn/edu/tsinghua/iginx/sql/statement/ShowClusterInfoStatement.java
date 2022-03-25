package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoReq;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoResp;

public class ShowClusterInfoStatement extends SystemStatement {

  public ShowClusterInfoStatement() {
    this.statementType = StatementType.SHOW_CLUSTER_INFO;
  }

  @Override
  public void execute(RequestContext ctx) {
    IginxWorker worker = IginxWorker.getInstance();
    GetClusterInfoReq req = new GetClusterInfoReq(ctx.getSessionId());
    GetClusterInfoResp getClusterInfoResp = worker.getClusterInfo(req);

    Result result = new Result(getClusterInfoResp.getStatus());
    result.setIginxInfos(getClusterInfoResp.getIginxInfos());
    result.setStorageEngineInfos(getClusterInfoResp.getStorageEngineInfos());
    result.setMetaStorageInfos(getClusterInfoResp.getMetaStorageInfos());
    result.setLocalMetaStorageInfo(getClusterInfoResp.getLocalMetaStorageInfo());
    ctx.setResult(result);
  }
}
