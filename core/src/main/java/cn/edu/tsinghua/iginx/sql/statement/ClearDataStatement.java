package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.SortUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ClearDataStatement extends Statement {

    public ClearDataStatement() {
        this.statementType = StatementType.CLEAR_DATA;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) {
        List<String> paths = new ArrayList<>(Arrays.asList("*"));
        IginxWorker worker = IginxWorker.getInstance();
        DeleteColumnsReq req = new DeleteColumnsReq(sessionId, SortUtils.mergeAndSortPaths(paths));
        return new ExecuteSqlResp(worker.deleteColumns(req), SqlType.ClearData);
    }
}
