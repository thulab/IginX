package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

import java.util.Collections;

public class ShowReplicationStatement extends SystemStatement {

    public ShowReplicationStatement() {
        this.statementType = StatementType.SHOW_REPLICATION;
    }

    @Override
    public void execute(RequestContext ctx) {
        Result result = new Result(RpcUtils.SUCCESS);

        int num = ConfigDescriptor.getInstance().getConfig().getReplicaNum() + 1;

        if (ctx.isUseStream()) {
            Header header = new Header(Collections.singletonList(new Field("replica", DataType.INTEGER)));
            RowStream table = new Table(header, Collections.singletonList(new Row(header, new Integer[]{num})));
            result.setResultStream(table);
        } else {
            result.setReplicaNum(num);
        }
        ctx.setResult(result);
    }
}
