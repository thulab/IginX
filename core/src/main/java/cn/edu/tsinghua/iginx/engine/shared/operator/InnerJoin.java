package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class InnerJoin extends AbstractBinaryOperator {

    private final Filter filter;

    private final List<String> joinColumns;

    private final JoinAlgType joinAlgType;

    private final boolean isNaturalJoin;

    public InnerJoin(Source sourceA, Source sourceB, Filter filter, List<String> joinColumns) {
        this(sourceA, sourceB, filter, joinColumns, false);
    }

    public InnerJoin(Source sourceA, Source sourceB, Filter filter, List<String> joinColumns,
        boolean isNaturalJoin) {
        this(sourceA, sourceB, filter, joinColumns, isNaturalJoin, JoinAlgType.HashJoin);
    }

    public InnerJoin(Source sourceA, Source sourceB, Filter filter, List<String> joinColumns,
        boolean isNaturalJoin, JoinAlgType joinAlgType) {
        super(OperatorType.InnerJoin, sourceA, sourceB);
        this.filter = filter;
        this.joinColumns = joinColumns;
        this.joinAlgType = joinAlgType;
        this.isNaturalJoin = isNaturalJoin;
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

    public boolean isNaturalJoin() {
        return isNaturalJoin;
    }
    
    @Override
    public Operator copy() {
        return new InnerJoin(getSourceA().copy(), getSourceB().copy(), filter.copy(),
            new ArrayList<>(joinColumns), isNaturalJoin, joinAlgType);
    }
}
