package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.SortUtils;

import java.util.ArrayList;
import java.util.List;

public class DeleteStatement extends Statement {

    private List<String> paths;
    private long startTime;
    private long endTime;

    public DeleteStatement() {
        this.statementType = StatementType.DELETE;
        paths = new ArrayList<>();
        startTime = Long.MIN_VALUE;
        endTime = Long.MAX_VALUE;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void addPath(String path) {
        paths.add(path);
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long time) {
        this.startTime = time;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long time) {
        this.endTime = time;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) {
        IginxWorker worker = IginxWorker.getInstance();
        DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(
                sessionId,
                SortUtils.mergeAndSortPaths(paths),
                startTime,
                endTime
        );
        return new ExecuteSqlResp(worker.deleteDataInColumns(req), SqlType.Delete);
    }
}
