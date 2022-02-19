package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

public class ShowReplicationStatement extends SystemStatement {

    public ShowReplicationStatement() {
        this.statementType = StatementType.SHOW_REPLICATION;
    }

    @Override
    public void execute(RequestContext ctx) {
        Result result = new Result(RpcUtils.SUCCESS);
        result.setReplicaNum(ConfigDescriptor.getInstance().getConfig().getReplicaNum() + 1);
        ctx.setResult(result);
    }
}
