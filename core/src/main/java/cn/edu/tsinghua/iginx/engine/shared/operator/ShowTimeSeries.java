package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;

import java.util.HashSet;
import java.util.Set;

public class ShowTimeSeries extends AbstractUnaryOperator {

    private final Set<String> pathRegexSet;
    private final TagFilter tagFilter;

    private final int limit;
    private final int offset;

    public ShowTimeSeries(GlobalSource source, Set<String> pathRegexSet, TagFilter tagFilter, int limit, int offset) {
        super(OperatorType.ShowTimeSeries, source);
        this.pathRegexSet = pathRegexSet;
        this.tagFilter = tagFilter;
        this.limit = limit;
        this.offset = offset;
    }

    public Set<String> getPathRegexSet() {
        return pathRegexSet;
    }

    public TagFilter getTagFilter() {
        return tagFilter;
    }

    public int getLimit() {
        return limit;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public Operator copy() {
        return new ShowTimeSeries((GlobalSource) getSource().copy(), new HashSet<>(pathRegexSet), tagFilter.copy(), limit, offset);
    }
}
