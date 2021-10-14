package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.SortUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClearDataOperator extends Operator {

    public ClearDataOperator() {
        this.operatorType = OperatorType.CLEAR_DATA;
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        List<String> paths = new ArrayList<>(Arrays.asList("*"));
        IginxWorker worker = IginxWorker.getInstance();
        DeleteColumnsReq req = new DeleteColumnsReq(sessionId, SortUtils.mergeAndSortPaths(paths));
        return new ExecuteSqlResp(worker.deleteColumns(req), SqlType.ClearData);
    }
}
