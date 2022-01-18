package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.source.Source;

import java.util.List;

public class SetCombine extends AbstractMultipleOperator {

    public SetCombine(List<Source> sources) {
        super(OperatorType.SetCombine, sources);
    }

    @Override
    public Operator copy() {
        return new SetCombine(getSources());
    }
}
