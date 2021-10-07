package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoReq;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;

public class ShowClusterInfoOperator extends Operator {

    public ShowClusterInfoOperator() {
        this.operatorType = OperatorType.SHOW_CLUSTER_INFO;
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        IginxWorker worker = IginxWorker.getInstance();
        GetClusterInfoReq req = new GetClusterInfoReq(sessionId);
        GetClusterInfoResp getClusterInfoResp = worker.getClusterInfo(req);

        ExecuteSqlResp resp = new ExecuteSqlResp(getClusterInfoResp.getStatus(), SqlType.ShowClusterInfo);
        resp.setIginxInfos(getClusterInfoResp.getIginxInfos());
        resp.setStorageEngineInfos(getClusterInfoResp.getStorageEngineInfos());
        resp.setMetaStorageInfos(getClusterInfoResp.getMetaStorageInfos());
        resp.setLocalMetaStorageInfo(getClusterInfoResp.getLocalMetaStorageInfo());
        return resp;
    }
}
