package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class OuterJoin extends AbstractBinaryOperator {

    private final OuterJoinType outerJoinType;

    private final Filter filter;

    private final List<String> joinColumns;

    private final JoinAlgType joinAlgType;

    public OuterJoin(Source sourceA, Source sourceB, OuterJoinType outerJoinType, Filter filter,
        List<String> joinColumns) {
        this(sourceA, sourceB, outerJoinType, filter, joinColumns, JoinAlgType.HashJoin);
    }

    public OuterJoin(Source sourceA, Source sourceB, OuterJoinType outerJoinType, Filter filter,
        List<String> joinColumns, JoinAlgType joinAlgType) {
        super(OperatorType.OuterJoin, sourceA, sourceB);
        this.outerJoinType = outerJoinType;
        this.filter = filter;
        this.joinColumns = joinColumns;
        this.joinAlgType = joinAlgType;
    }

    public OuterJoinType getOuterJoinType() {
        return outerJoinType;
    }

    public Filter getFilter() {
        return filter;
    }

    public List<String> getJoinColumns() {
        return joinColumns;
    }

    public JoinAlgType getJoinAlgType() {
        return joinAlgType;
    }

    @Override
    public Operator copy() {
        return new OuterJoin(getSourceA().copy(), getSourceB().copy(), outerJoinType, filter.copy(),
            new ArrayList<>(joinColumns), joinAlgType);
    }
}
