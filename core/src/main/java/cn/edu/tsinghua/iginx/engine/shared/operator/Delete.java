package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;

import java.util.ArrayList;
import java.util.List;

public class Delete extends AbstractUnaryOperator {

    private final Filter filter;
    private final List<String> patterns;

    public Delete(FragmentSource source, Filter filter, List<String> patterns) {
        super(OperatorType.Delete, source);
        this.filter = filter;
        this.patterns = patterns;
    }

    public Filter getFilter() {
        return filter;
    }

    public List<String> getPatterns() {
        return patterns;
    }

    @Override
    public Operator copy() {
        return new Delete((FragmentSource) getSource().copy(), filter.copy(), new ArrayList<>(patterns));
    }
}
