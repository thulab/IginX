package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

import java.util.Map;
import java.util.StringJoiner;

public class BasePreciseTagFilter implements TagFilter {

    private final Map<String, String> tags;

    public BasePreciseTagFilter(Map<String, String> tags) {
        this.tags = tags;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public TagFilterType getType() {
        return TagFilterType.BasePrecise;
    }

    @Override
    public TagFilter copy() {
        return new BasePreciseTagFilter(tags);
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(" && ", "(", ")");
        tags.forEach((k, v) -> joiner.add(k + "=" + v));
        return joiner.toString();
    }
}
