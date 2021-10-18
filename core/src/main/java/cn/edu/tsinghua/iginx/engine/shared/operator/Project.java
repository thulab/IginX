package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.util.List;

public class Project extends AbstractUnaryOperator {

    private final List<String> patterns;

    public Project(Source source, List<String> patterns) {
        super(OperatorType.Project, source);
        if (patterns == null) {
            throw new IllegalArgumentException("patterns shouldn't be null");
        }
        this.patterns = patterns;
    }

    public List<String> getPatterns() {
        return patterns;
    }
}
