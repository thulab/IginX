package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

public class ShowReplicationOperator extends Operator {

    public ShowReplicationOperator() {
        this.operatorType = OperatorType.SHOW_REPLICATION;
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.GetReplicaNum);
        resp.setReplicaNum(ConfigDescriptor.getInstance().getConfig().getReplicaNum() + 1);
        return resp;
    }
}
