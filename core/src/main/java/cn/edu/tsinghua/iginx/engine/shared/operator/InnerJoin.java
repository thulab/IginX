package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class InnerJoin extends AbstractBinaryOperator {

    private final String joinColumnA;

    private final String joinColumnB;

    private final Filter extraFilter;

    private final JoinAlgType joinAlgType;

    public InnerJoin(Source sourceA, Source sourceB, String joinColumnA, String joinColumnB) {
        this(sourceA, sourceB, joinColumnA, joinColumnB, null, JoinAlgType.HashJoin);
    }

    public InnerJoin(Source sourceA, Source sourceB, String joinColumnA, String joinColumnB,
        Filter extraFilter, JoinAlgType joinAlgType) {
        super(OperatorType.InnerJoin, sourceA, sourceB);
        this.joinColumnA = joinColumnA;
        this.joinColumnB = joinColumnB;
        this.extraFilter = extraFilter;
        this.joinAlgType = joinAlgType;
    }

    public String getJoinColumnA() {
        return joinColumnA;
    }

    public String getJoinColumnB() {
        return joinColumnB;
    }

    public Filter getExtraFilter() {
        return extraFilter;
    }

    @Override
    public Operator copy() {
        return new InnerJoin(getSourceA().copy(), getSourceB().copy(), joinColumnA, joinColumnB,
            extraFilter.copy(), joinAlgType);
    }
}
