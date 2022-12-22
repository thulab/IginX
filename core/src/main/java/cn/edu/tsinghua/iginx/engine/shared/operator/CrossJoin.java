package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class CrossJoin extends AbstractBinaryOperator {

    public CrossJoin(Source sourceA, Source sourceB) {
        super(OperatorType.CrossJoin, sourceA, sourceB);
    }

    @Override
    public Operator copy() {
        return new CrossJoin(getSourceA().copy(), getSourceB().copy());
    }
}
