package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import java.util.ArrayList;
import java.util.List;

public class OuterJoin extends AbstractBinaryOperator {

    private final String prefixA;

    private final String prefixB;

    private final OuterJoinType outerJoinType;

    private final Filter filter;

    private final List<String> joinColumns;

    private final JoinAlgType joinAlgType;

    private final boolean isNaturalJoin;

    public OuterJoin(Source sourceA, Source sourceB, String prefixA, String prefixB,
        OuterJoinType outerJoinType, Filter filter, List<String> joinColumns) {
        this(sourceA, sourceB, prefixA, prefixB, outerJoinType, filter, joinColumns, false, JoinAlgType.HashJoin);
    }

    public OuterJoin(Source sourceA, Source sourceB, String prefixA, String prefixB,
        OuterJoinType outerJoinType, Filter filter, List<String> joinColumns,
        boolean isNaturalJoin, JoinAlgType joinAlgType) {
        super(OperatorType.OuterJoin, sourceA, sourceB);
        this.prefixA = prefixA;
        this.prefixB = prefixB;
        this.outerJoinType = outerJoinType;
        this.filter = filter;
        if (joinColumns != null) {
            this.joinColumns = joinColumns;
        } else {
            this.joinColumns = new ArrayList<>();
        }
        this.joinAlgType = joinAlgType;
        this.isNaturalJoin = isNaturalJoin;
    }

    public String getPrefixA() {
        return prefixA;
    }

    public String getPrefixB() {
        return prefixB;
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

    public boolean isNaturalJoin() {
        return isNaturalJoin;
    }
    
    @Override
    public Operator copy() {
        return new OuterJoin(getSourceA().copy(), getSourceB().copy(), prefixA, prefixB,
            outerJoinType, filter.copy(), new ArrayList<>(joinColumns), isNaturalJoin, joinAlgType);
    }
}
