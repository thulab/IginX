package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PreciseTagFilter implements TagFilter {

    private final List<BasePreciseTagFilter> children;

    public PreciseTagFilter(List<BasePreciseTagFilter> children) {
        this.children = children;
    }

    public List<BasePreciseTagFilter> getChildren() {
        return children;
    }

    @Override
    public TagFilterType getType() {
        return TagFilterType.Precise;
    }

    @Override
    public TagFilter copy() {
        List<BasePreciseTagFilter> newChildren = new ArrayList<>();
        children.forEach(e -> newChildren.add((BasePreciseTagFilter) e.copy()));
        return new PreciseTagFilter(newChildren);
    }

    @Override
    public String toString() {
        return children.stream().map(Object::toString).collect(Collectors.joining(" || ", "(", ")"));
    }
}
