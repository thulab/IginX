package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

public class WithoutTagFilter implements TagFilter {

    public WithoutTagFilter() {
    }

    @Override
    public TagFilterType getType() {
        return TagFilterType.WithoutTag;
    }

    @Override
    public TagFilter copy() {
        return new WithoutTagFilter();
    }
}
