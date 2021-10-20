package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.SortUtils;

import java.util.ArrayList;
import java.util.List;

public class DeleteTimeSeriesOperator extends Operator {

    private List<String> paths;

    public DeleteTimeSeriesOperator() {
        this.operatorType = OperatorType.DELETE_TIME_SERIES;
        paths = new ArrayList<>();
    }

    public List<String> getPaths() {
        return paths;
    }

    public void addPath(String path) {
        this.paths.add(path);
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        IginxWorker worker = IginxWorker.getInstance();
        DeleteColumnsReq req = new DeleteColumnsReq(sessionId, SortUtils.mergeAndSortPaths(paths));
        return new ExecuteSqlResp(worker.deleteColumns(req), SqlType.DeleteTimeSeries);
    }
}
