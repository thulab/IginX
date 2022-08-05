package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;

import java.util.HashSet;
import java.util.Set;

public class ShowTimeSeries extends AbstractUnaryOperator {

    private final Set<String> pathRegexSet;
    private final TagFilter tagFilter;

    public ShowTimeSeries(GlobalSource source, Set<String> pathRegexSet, TagFilter tagFilter) {
        super(OperatorType.ShowTimeSeries, source);
        this.pathRegexSet = pathRegexSet;
        this.tagFilter = tagFilter;
    }

    public Set<String> getPathRegexSet() {
        return pathRegexSet;
    }

    public TagFilter getTagFilter() {
        return tagFilter;
    }

    @Override
    public Operator copy() {
        return new ShowTimeSeries((GlobalSource) getSource().copy(), new HashSet<>(pathRegexSet), tagFilter.copy());
    }
}
