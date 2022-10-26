package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.util.ArrayList;
import java.util.List;

public class Reorder extends AbstractUnaryOperator {

    private final List<String> patterns;

    public Reorder(Source source, List<String> patterns) {
        super(OperatorType.Reorder, source);
        if (patterns == null) {
            throw new IllegalArgumentException("patterns shouldn't be null");
        }
        this.patterns = patterns;
    }

    @Override
    public Operator copy() {
        return new Reorder(getSource().copy(), new ArrayList<>(patterns));
    }

    public List<String> getPatterns() {
        return patterns;
    }
}
