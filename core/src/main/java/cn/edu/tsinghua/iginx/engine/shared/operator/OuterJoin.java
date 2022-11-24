package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class OuterJoin extends AbstractBinaryOperator {

    private final OuterJoinType outerJoinType;

    private final String joinColumnA;

    private final String joinColumnB;

    private final Filter extraFilter;

    private final JoinAlgType joinAlgType;

    public OuterJoin(Source sourceA, Source sourceB, String joinColumnA, String joinColumnB,
        OuterJoinType outerJoinType) {
        this(sourceA, sourceB, joinColumnA, joinColumnB, outerJoinType,
            null, JoinAlgType.HashJoin);
    }

    public OuterJoin(Source sourceA, Source sourceB, String joinColumnA, String joinColumnB,
        OuterJoinType outerJoinType, Filter extraFilter, JoinAlgType joinAlgType) {
        super(OperatorType.OuterJoin, sourceA, sourceB);
        this.outerJoinType = outerJoinType;
        this.joinColumnA = joinColumnA;
        this.joinColumnB = joinColumnB;
        this.extraFilter = extraFilter;
        this.joinAlgType = joinAlgType;
    }

    public OuterJoinType getOuterJoinType() {
        return outerJoinType;
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
        return new OuterJoin(getSourceA().copy(), getSourceB().copy(), joinColumnA, joinColumnB,
            outerJoinType, extraFilter.copy(), joinAlgType);
    }

    enum OuterJoinType {
        LEFT,
        RIGHT,
        FULL
    }
}
