package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

public class ShowReplicationStatement extends Statement {

    public ShowReplicationStatement() {
        this.statementType = StatementType.SHOW_REPLICATION;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) {
        ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.GetReplicaNum);
        resp.setReplicaNum(ConfigDescriptor.getInstance().getConfig().getReplicaNum() + 1);
        return resp;
    }
}
