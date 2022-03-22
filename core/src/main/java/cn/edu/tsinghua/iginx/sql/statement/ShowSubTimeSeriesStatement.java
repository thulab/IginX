package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.ShowSubPathsReq;
import cn.edu.tsinghua.iginx.thrift.ShowSubPathsResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;

public class ShowSubTimeSeriesStatement extends Statement {

    private String prefixPath;

    public ShowSubTimeSeriesStatement(String prefixPath) {
        this.statementType = StatementType.SHOW_SUB_TIMESERIES;
        this.prefixPath = prefixPath;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) {
        IginxWorker worker = IginxWorker.getInstance();
        ShowSubPathsReq req = new ShowSubPathsReq(sessionId);
        if (prefixPath != null && !prefixPath.equals("")) {
            req.setPath(prefixPath);
        }

        ShowSubPathsResp showSubPathsResp = worker.showSubPaths(req);
        ExecuteSqlResp resp = new ExecuteSqlResp(showSubPathsResp.getStatus(), SqlType.ShowSubTimeSeries);

        resp.setPaths(showSubPathsResp.getPaths());
        return resp;
    }
}
