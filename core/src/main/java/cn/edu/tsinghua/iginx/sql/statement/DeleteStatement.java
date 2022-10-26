package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;

import java.util.ArrayList;
import java.util.List;

public class DeleteStatement extends DataStatement {

    private boolean deleteAll;  // delete data & path

    private List<String> paths;
    private List<TimeRange> timeRanges;
    private TagFilter tagFilter;

    private boolean involveDummyData;

    public DeleteStatement() {
        this.statementType = StatementType.DELETE;
        this.paths = new ArrayList<>();
        this.timeRanges = new ArrayList<>();
        this.deleteAll = false;
        this.tagFilter = null;
        this.involveDummyData = false;
    }

    public DeleteStatement(List<String> paths, long startTime, long endTime) {
        this.statementType = StatementType.DELETE;
        this.paths = paths;
        this.timeRanges = new ArrayList<>();
        this.timeRanges.add(new TimeRange(startTime, endTime));
        this.deleteAll = false;
        this.tagFilter = null;
        this.involveDummyData = false;
    }

    public DeleteStatement(List<String> paths) {
        this(paths, null);
    }

    public DeleteStatement(List<String> paths, TagFilter tagFilter) {
        this.statementType = StatementType.DELETE;
        this.paths = paths;
        this.timeRanges = new ArrayList<>();
        this.deleteAll = true;
        this.tagFilter = tagFilter;
        this.involveDummyData = false;
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

    public TagFilter getTagFilter() {
        return tagFilter;
    }

    public void setTagFilter(TagFilter tagFilter) {
        this.tagFilter = tagFilter;
    }

    public boolean isInvolveDummyData() {
        return involveDummyData;
    }

    public void setInvolveDummyData(boolean involveDummyData) {
        this.involveDummyData = involveDummyData;
    }

    public void setTimeRangesByFilter(Filter filter) {
        if (filter != null) {
            this.timeRanges = ExprUtils.getTimeRangesFromFilter(filter);
            if (timeRanges.isEmpty()) {
                throw new SQLParserException("This clause delete nothing, check your filter again.");
            }
        }
    }

    public boolean isDeleteAll() {
        return deleteAll;
    }

    public void setDeleteAll(boolean deleteAll) {
        this.deleteAll = deleteAll;
    }
}
