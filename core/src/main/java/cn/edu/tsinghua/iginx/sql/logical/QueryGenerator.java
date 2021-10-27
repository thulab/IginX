package cn.edu.tsinghua.iginx.sql.logical;

import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.utils.SortUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryGenerator implements LogicalGenerator {

    private final static QueryGenerator instance = new QueryGenerator();

    private final static FunctionManager functionManager = FunctionManager.getInstance();

    private final List<Optimizer> optimizerList = new ArrayList<>();

    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();

    private QueryGenerator() {
    }

    public static QueryGenerator getInstance() {
        return instance;
    }

    public void registerOptimizer(Optimizer optimizer) {
        if (optimizer != null)
            optimizerList.add(optimizer);
    }

    @Override
    public Operator generate(Statement statement) {
        if (statement == null)
            return null;
        if (statement.getType() != Statement.StatementType.SELECT)
            return null;
        Operator root = generateRoot((SelectStatement) statement);
        for (Optimizer optimizer : optimizerList) {
            root = optimizer.optimize(root);
        }
        return root;
    }

    private Operator generateRoot(SelectStatement statement) {
        List<String> pathList = SortUtils.mergeAndSortPaths(new ArrayList<>(statement.getPathSet()));

        TimeSeriesInterval interval = new TimeSeriesInterval(pathList.get(0), pathList.get(pathList.size() - 1));
        Map<TimeSeriesInterval, List<FragmentMeta>> fragments = metaManager.getFragmentMapByTimeSeriesInterval(interval);

        List<Operator> joinList = new ArrayList<>();
        fragments.forEach((k, v) -> {
            List<Operator> unionList = new ArrayList<>();
            v.forEach(meta -> unionList.add(new Project(new FragmentSource(meta), pathList)));
            joinList.add(unionOperators(unionList));
        });

        Select select = new Select(new OperatorSource(joinOperators(joinList)), statement.getFilter());

        List<Operator> queryList = new ArrayList<>();
        if (statement.hasGroupBy()) {
            // DownSample Query
            statement.getSelectedFuncsAndPaths().forEach((k, v) -> {
                List<Value> wrappedPath = new ArrayList<>();
                v.forEach(str -> wrappedPath.add(new Value(str)));
                Operator copySelect = select.copy();
                queryList.add(
                        new Downsample(
                                new OperatorSource(copySelect),
                                statement.getPrecision(),
                                new FunctionCall(functionManager.getFunction(k), wrappedPath),
                                new TimeRange(0, Long.MAX_VALUE)
                        )
                );
            });
        } else if (statement.hasFunc()) {
            // Aggregate Query
            statement.getSelectedFuncsAndPaths().forEach((k, v) -> {
                List<Value> wrappedPath = new ArrayList<>();
                v.forEach(str -> wrappedPath.add(new Value(str)));
                Operator copySelect = select.copy();
                queryList.add(
                        new SetTransform(
                                new OperatorSource(copySelect),
                                new FunctionCall(functionManager.getFunction(k), wrappedPath)
                        )
                );
            });
        } else {
            List<String> selectedPath = new ArrayList<>();
            statement.getSelectedFuncsAndPaths().forEach((k, v) -> selectedPath.addAll(v));
            queryList.add(new Project(new OperatorSource(select), selectedPath));
        }

        Operator root = joinOperators(queryList);

        if (!statement.getOrderByPath().equals("")) {
            root = new Sort(
                    new OperatorSource(root),
                    statement.getOrderByPath(),
                    statement.isAscending() ? Sort.SortType.ASC : Sort.SortType.DESC
            );
        }

        root = new Limit(
                new OperatorSource(root),
                (int) statement.getLimit(),
                (int) statement.getOffset()
        );

        return root;
    }

    private Operator unionOperators(List<Operator> operators) {
        if (operators == null || operators.isEmpty())
            return null;
        if (operators.size() == 1)
            return operators.get(0);
        Operator union = operators.get(0);
        for (int i = 1; i < operators.size(); i++) {
            union = new Union(new OperatorSource(union), new OperatorSource(operators.get(i)));
        }
        return union;
    }

    private Operator joinOperators(List<Operator> operators) {
        if (operators == null || operators.isEmpty())
            return null;
        if (operators.size() == 1)
            return operators.get(0);
        Operator join = operators.get(0);
        for (int i = 1; i < operators.size(); i++) {
            join = new Join(new OperatorSource(join), new OperatorSource(operators.get(i)));
        }
        return join;
    }
}
