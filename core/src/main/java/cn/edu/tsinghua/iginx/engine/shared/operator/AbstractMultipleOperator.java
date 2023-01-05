package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.util.List;

public abstract class AbstractMultipleOperator extends AbstractOperator implements MultipleOperator {

    private List<Source> sources;

    public AbstractMultipleOperator(OperatorType type, List<Source> sources) {
        super(type);
        if (sources == null || sources.isEmpty()) {
            throw new IllegalArgumentException("sourceList shouldn't be null or empty");
        }
        sources.forEach(source -> {
            if (source == null) {
                throw new IllegalArgumentException("source shouldn't be null");
            }
        });
        this.sources = sources;
    }

    public AbstractMultipleOperator(List<Source> sources) {
        this(OperatorType.Multiple, sources);
    }

    @Override
    public List<Source> getSources() {
        return sources;
    }

    @Override
    public void setSources(List<Source> sources) {
        this.sources = sources;
    }
}
