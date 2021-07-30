package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.DeleteDataInColumnsContext;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.SortUtils;

import java.util.ArrayList;
import java.util.List;

public class DeleteOperator extends Operator {

    private List<String> paths;
    private long startTime;
    private long endTime;

    public DeleteOperator() {
        this.operatorType = OperatorType.DELETE;
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
    public ExecuteSqlResp doOperation(long sessionId) {
        Core core = Core.getInstance();
        DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(
                sessionId,
                SortUtils.mergeAndSortPaths(paths),
                startTime,
                endTime
        );
        DeleteDataInColumnsContext ctx = new DeleteDataInColumnsContext(req);
        core.processRequest(ctx);
        return new ExecuteSqlResp(ctx.getStatus(), SqlType.Delete);
    }
}
