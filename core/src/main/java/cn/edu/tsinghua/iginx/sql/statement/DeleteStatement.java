package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.sql.logical.ExprUtils;

import java.util.ArrayList;
import java.util.List;

public class DeleteStatement extends DataStatement {

    private List<String> paths;
    private List<TimeRange> timeRanges;
    private boolean deleteAll;  // delete data & path

    public DeleteStatement() {
        this.statementType = StatementType.DELETE;
        this.paths = new ArrayList<>();
        this.timeRanges = new ArrayList<>();
        this.deleteAll = false;
    }

    public DeleteStatement(List<String> paths, long startTime, long endTime) {
        this.statementType = StatementType.DELETE;
        this.paths = paths;
        this.timeRanges = new ArrayList<>();
        this.timeRanges.add(new TimeRange(startTime, endTime));
        this.deleteAll = false;
    }

    public DeleteStatement(List<String> paths) {
        this.statementType = StatementType.DELETE;
        this.paths = paths;
        this.timeRanges = new ArrayList<>();
        this.deleteAll = true;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void addPath(String path) {
        paths.add(path);
    }

    public List<TimeRange> getTimeRanges() {
        return timeRanges;
    }

    public void setTimeRanges(List<TimeRange> timeRanges) {
        this.timeRanges = timeRanges;
    }

    public void setTimeRangesByFilter(Filter filter) {
        if (filter != null) {
            this.timeRanges = ExprUtils.getTimeRangesFromFilter(filter);
        }
    }

    public boolean isDeleteAll() {
        return deleteAll;
    }

    public void setDeleteAll(boolean deleteAll) {
        this.deleteAll = deleteAll;
    }
}
