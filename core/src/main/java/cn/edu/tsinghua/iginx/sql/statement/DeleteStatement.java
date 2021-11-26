package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.sql.logical.ExprUtils;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;

import java.util.ArrayList;
import java.util.List;

public class DeleteStatement extends DataStatement {

    private List<String> paths;
    private Filter filter;
    private List<TimeRange> timeRanges;

    public DeleteStatement() {
        this.statementType = StatementType.DELETE;
        paths = new ArrayList<>();
        timeRanges = new ArrayList<>();
    }

    public List<String> getPaths() {
        return paths;
    }

    public void addPath(String path) {
        paths.add(path);
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
        if (filter != null) {
            this.timeRanges = ExprUtils.getTimeRangesFromFilter(filter);
        }
    }

    public List<TimeRange> getTimeRanges() {
        return timeRanges;
    }

    public void setTimeRanges(List<TimeRange> timeRanges) {
        this.timeRanges = timeRanges;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        throw new ExecutionException("Delete statement can not be executed directly.");
    }
}
