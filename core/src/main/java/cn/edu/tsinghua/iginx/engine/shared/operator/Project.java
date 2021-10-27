package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.io.Serializable;
import java.util.ArrayList;
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

    @Override
    public Operator copy() {
        return new Project(getSource().copy(), new ArrayList<>(patterns));
    }
}
