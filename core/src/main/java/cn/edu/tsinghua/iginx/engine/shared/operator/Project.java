package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.util.ArrayList;
import java.util.List;

public class Project extends AbstractUnaryOperator {

    private final List<String> patterns;

    private final TagFilter tagFilter;

    public Project(Source source, List<String> patterns, TagFilter tagFilter) {
        super(OperatorType.Project, source);
        if (patterns == null) {
            throw new IllegalArgumentException("patterns shouldn't be null");
        }
        this.patterns = patterns;
        this.tagFilter = tagFilter;
    }

    public List<String> getPatterns() {
        return patterns;
    }

    @Override
    public Operator copy() {
        return new Project(getSource().copy(), new ArrayList<>(patterns), tagFilter.copy());
    }
}
