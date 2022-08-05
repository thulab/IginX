package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;

import java.util.HashSet;
import java.util.Set;

public class ShowTimeSeriesStatement extends DataStatement {

    private Set<String> pathRegexSet;
    private TagFilter tagFilter;

    public ShowTimeSeriesStatement() {
        this.statementType = StatementType.SHOW_TIME_SERIES;
        this.pathRegexSet = new HashSet<>();
    }

    public void setPathRegex(String pathRegex) {
        this.pathRegexSet.add(pathRegex);
    }

    public Set<String> getPathRegexSet() {
        return pathRegexSet;
    }

    public void setPathRegexSet(Set<String> pathRegexSet) {
        this.pathRegexSet = pathRegexSet;
    }

    public TagFilter getTagFilter() {
        return tagFilter;
    }

    public void setTagFilter(TagFilter tagFilter) {
        this.tagFilter = tagFilter;
    }
}
