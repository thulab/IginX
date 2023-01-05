package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class CrossJoin extends AbstractBinaryOperator {

    private final String prefixA;

    private final String prefixB;

    public CrossJoin(Source sourceA, Source sourceB, String prefixA, String prefixB) {
        super(OperatorType.CrossJoin, sourceA, sourceB);
        this.prefixA = prefixA;
        this.prefixB = prefixB;
    }

    public String getPrefixA() {
        return prefixA;
    }

    public String getPrefixB() {
        return prefixB;
    }

    @Override
    public Operator copy() {
        return new CrossJoin(getSourceA().copy(), getSourceB().copy(), prefixA, prefixB);
    }
}
