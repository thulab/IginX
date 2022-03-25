package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.LogicalOptimizerManager;
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
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.ORDINAL;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.TIMESTAMP;

public class QueryGenerator extends AbstractGenerator {

    private static final Logger logger = LoggerFactory.getLogger(QueryGenerator.class);
    private final static Config config = ConfigDescriptor.getInstance().getConfig();
    private final static QueryGenerator instance = new QueryGenerator();
    private final static FunctionManager functionManager = FunctionManager.getInstance();
    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();
    private final IPolicy policy = PolicyManager.getInstance()
        .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

    private QueryGenerator() {
        this.type = GeneratorType.Query;
        LogicalOptimizerManager optimizerManager = LogicalOptimizerManager.getInstance();
        String[] optimizers = config.getQueryOptimizer().split(",");
        for (String optimizer : optimizers) {
            registerOptimizer(optimizerManager.getOptimizer(optimizer));
        }
    }

    public static QueryGenerator getInstance() {
        return instance;
    }

    protected Operator generateRoot(Statement statement) {
        SelectStatement selectStatement = (SelectStatement) statement;

        policy.notify(selectStatement);

        List<String> pathList = SortUtils.mergeAndSortPaths(new ArrayList<>(selectStatement.getPathSet()));

        TimeSeriesInterval interval = new TimeSeriesInterval(pathList.get(0), pathList.get(pathList.size() - 1));

        Map<TimeSeriesInterval, List<FragmentMeta>> fragments = metaManager.getFragmentMapByTimeSeriesInterval(interval);
        if (fragments.isEmpty()) {
            //on startup
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.generateInitialFragmentsAndStorageUnits(selectStatement);
            metaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            fragments = metaManager.getFragmentMapByTimeSeriesInterval(interval);
        }

        List<Operator> joinList = new ArrayList<>();
        fragments.forEach((k, v) -> {
            List<Operator> unionList = new ArrayList<>();
            v.forEach(meta -> unionList.add(new Project(new FragmentSource(meta), pathList)));
            joinList.add(unionOperators(unionList));
        });

        Operator root = joinOperatorsByTime(joinList);

        if (selectStatement.hasValueFilter()) {
            root = new Select(new OperatorSource(root), selectStatement.getFilter());
        }

        List<Operator> queryList = new ArrayList<>();
        if (selectStatement.getQueryType() == SelectStatement.QueryType.DownSampleQuery) {
            // DownSample Query
            Operator finalRoot = root;
            selectStatement.getSelectedFuncsAndPaths().forEach((k, v) -> v.forEach(str -> {
                List<Value> params = new ArrayList<>();
                params.add(new Value(str));
                if (!selectStatement.getLayers().isEmpty()) {
                    params.add(new Value(selectStatement.getLayers().stream().map(String::valueOf).collect(Collectors.joining(","))));
                }
                Operator copySelect = finalRoot.copy();

                queryList.add(
                    new Downsample(
                        new OperatorSource(copySelect),
                        selectStatement.getPrecision(),
                        new FunctionCall(functionManager.getFunction(k), params),
                        new TimeRange(selectStatement.getStartTime(), selectStatement.getEndTime())
                    )
                );
            }));
        } else if (selectStatement.getQueryType() == SelectStatement.QueryType.AggregateQuery) {
            // Aggregate Query
            Operator finalRoot = root;
            selectStatement.getSelectedFuncsAndPaths().forEach((k, v) -> v.forEach(str -> {
                List<Value> params = new ArrayList<>();
                params.add(new Value(str));
                if (!selectStatement.getLayers().isEmpty()) {
                    params.add(new Value(selectStatement.getLayers().stream().map(String::valueOf).collect(Collectors.joining(","))));
                }
                Operator copySelect = finalRoot.copy();
                logger.info("function: " + k + ", wrapped path: " + v);
                queryList.add(
                    new SetTransform(
                        new OperatorSource(copySelect),
                        new FunctionCall(functionManager.getFunction(k), params)
                    )
                );
            }));
        } else if (selectStatement.getQueryType() == SelectStatement.QueryType.LastFirstQuery) {
            Operator finalRoot = root;
            selectStatement.getSelectedFuncsAndPaths().forEach((k, v) -> v.forEach(str -> {
                List<Value> params = new ArrayList<>(Collections.singletonList(new Value(str)));
                Operator copySelect = finalRoot.copy();
                logger.info("function: " + k + ", wrapped path: " + v);
                queryList.add(
                    new MappingTransform(
                        new OperatorSource(copySelect),
                        new FunctionCall(functionManager.getFunction(k), params)
                    )
                );
            }));
        } else {
            List<String> selectedPath = new ArrayList<>();
            selectStatement.getSelectedFuncsAndPaths().forEach((k, v) -> selectedPath.addAll(v));
            queryList.add(new Project(new OperatorSource(root), selectedPath));
        }

        if (selectStatement.getQueryType() == SelectStatement.QueryType.LastFirstQuery) {
            root = unionOperators(queryList);
        } else if (selectStatement.getQueryType() == SelectStatement.QueryType.DownSampleQuery) {
            root = joinOperatorsByTime(queryList);
        } else {
            root = joinOperators(queryList, ORDINAL);
        }

        if (!selectStatement.getOrderByPath().equals("")) {
            root = new Sort(
                new OperatorSource(root),
                selectStatement.getOrderByPath(),
                selectStatement.isAscending() ? Sort.SortType.ASC : Sort.SortType.DESC
            );
        }

        if (selectStatement.getLimit() != Integer.MAX_VALUE || selectStatement.getOffset() != 0) {
            root = new Limit(
                new OperatorSource(root),
                (int) selectStatement.getLimit(),
                (int) selectStatement.getOffset()
            );
        }

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

    private Operator joinOperatorsByTime(List<Operator> operators) {
        return joinOperators(operators, TIMESTAMP);
    }

    private Operator joinOperators(List<Operator> operators, String joinBy) {
        if (operators == null || operators.isEmpty())
            return null;
        if (operators.size() == 1)
            return operators.get(0);
        Operator join = operators.get(0);
        for (int i = 1; i < operators.size(); i++) {
            join = new Join(new OperatorSource(join), new OperatorSource(operators.get(i)), joinBy);
        }
        return join;
    }
}
