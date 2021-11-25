package cn.edu.tsinghua.iginx.sql.logical;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class InsertGenerator implements LogicalGenerator {

    private final GeneratorType type = GeneratorType.Insert;

    private static final Logger logger = LoggerFactory.getLogger(InsertGenerator.class);

    private final static InsertGenerator instance = new InsertGenerator();

    private final List<Optimizer> optimizerList = new ArrayList<>();

    private InsertGenerator() {
    }

    public static InsertGenerator getInstance() {
        return instance;
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
    public Operator generate(Statement statement) {
        if (statement == null)
            return null;
        if (statement.getType() != StatementType.INSERT)
            return null;
        Operator root = generateRoot((InsertStatement) statement);
        for (Optimizer optimizer : optimizerList) {
            root = optimizer.optimize(root);
        }
        return root;
    }

    private Operator generateRoot(InsertStatement statement) {

        return null;
    }
}
