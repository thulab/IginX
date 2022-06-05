package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Select extends AbstractUnaryOperator {

    private Filter filter;

    private TagFilter tagFilter;

    public Select(Source source, Filter filter, TagFilter tagFilter) {
        super(OperatorType.Select, source);
        if (filter == null) {
            throw new IllegalArgumentException("filter shouldn't be null");
        }
        this.filter = filter;
        this.tagFilter = tagFilter;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public TagFilter getTagFilter() {
        return tagFilter;
    }

    public void setTagFilter(TagFilter tagFilter) {
        this.tagFilter = tagFilter;
    }

    @Override
    public Operator copy() {
        return new Select(getSource().copy(), filter.copy(), tagFilter.copy());
    }
}
