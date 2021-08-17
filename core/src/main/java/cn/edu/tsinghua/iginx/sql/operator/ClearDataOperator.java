package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.DeleteColumnsContext;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.SortUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClearDataOperator extends Operator {

    public ClearDataOperator(){
        this.operatorType = OperatorType.CLEAR_DATA;
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        List<String> paths = new ArrayList<>(Arrays.asList("*"));
        Core core = Core.getInstance();
        DeleteColumnsReq req = new DeleteColumnsReq(sessionId, SortUtils.mergeAndSortPaths(paths));
        DeleteColumnsContext ctx = new DeleteColumnsContext(req);
        core.processRequest(ctx);
        return new ExecuteSqlResp(ctx.getStatus(), SqlType.ClearData);
    }
}
