package cn.edu.tsinghua.iginx.sql.logical;

import cn.edu.tsinghua.iginx.engine.shared.operator.CombineNonQuery;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DeleteGenerator implements LogicalGenerator {

    private final GeneratorType type = GeneratorType.Delete;

    private static final Logger logger = LoggerFactory.getLogger(InsertGenerator.class);

    private final static DeleteGenerator instance = new DeleteGenerator();

    private final List<Optimizer> optimizerList = new ArrayList<>();

    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();

    private DeleteGenerator() {
    }

    public static DeleteGenerator getInstance() {
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
        if (statement.getType() != StatementType.DELETE)
            return null;
        Operator root = generateRoot((DeleteStatement) statement);
        for (Optimizer optimizer : optimizerList) {
            root = optimizer.optimize(root);
        }
        return root;
    }

    private Operator generateRoot(DeleteStatement statement) {
        List<Delete> deleteList = new ArrayList<>();


        List<Source> sources = new ArrayList<>();
        deleteList.forEach(operator -> sources.add(new OperatorSource(operator)));
        return new CombineNonQuery(sources);
    }
}
