package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractGenerator implements LogicalGenerator {

    protected GeneratorType type;

    private final List<Optimizer> optimizerList = new ArrayList<>();
    private final static Map<GeneratorType, StatementType> typeMap = new HashMap<>();

    static {
        typeMap.put(GeneratorType.Query, StatementType.SELECT);
        typeMap.put(GeneratorType.Insert, StatementType.INSERT);
        typeMap.put(GeneratorType.Delete, StatementType.DELETE);
        typeMap.put(GeneratorType.ShowTimeSeries, StatementType.SHOW_TIME_SERIES);
    }

    public void registerOptimizer(Optimizer optimizer) {
        if (optimizer != null)
            optimizerList.add(optimizer);
    }

    @Override
    public GeneratorType getType() {
        return type;
    }

    @Override
    public Operator generate(RequestContext ctx) {
        Statement statement = ctx.getStatement();
        if (statement == null)
            return null;
        if (statement.getType() != typeMap.get(type))
            return null;
        Operator root = generateRoot(statement);
        for (Optimizer optimizer : optimizerList) {
            root = optimizer.optimize(root);
        }
        return root;
    }

    protected abstract Operator generateRoot(Statement statement);
}
