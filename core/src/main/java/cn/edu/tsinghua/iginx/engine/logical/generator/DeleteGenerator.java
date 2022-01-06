package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.logical.sampler.NaiveSampler;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.CombineNonQuery;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DeleteGenerator implements LogicalGenerator {

    private final GeneratorType type = GeneratorType.Delete;

    private static final Logger logger = LoggerFactory.getLogger(InsertGenerator.class);

    private final static DeleteGenerator instance = new DeleteGenerator();

    private final List<Optimizer> optimizerList = new ArrayList<>();

    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final IPolicy policy = PolicyManager.getInstance()
            .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

    private final NaiveSampler naiveSampler = NaiveSampler.getInstance();

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
        List<String> pathList = SortUtils.mergeAndSortPaths(new ArrayList<>(statement.getPaths()));

        naiveSampler.updatePrefix(new ArrayList<>(Arrays.asList(pathList.get(0), pathList.get(pathList.size()-1))));

        TimeSeriesInterval interval = new TimeSeriesInterval(pathList.get(0), pathList.get(pathList.size() - 1));

        Map<TimeSeriesInterval, List<FragmentMeta>> fragments = metaManager.getFragmentMapByTimeSeriesInterval(interval);
        if (fragments.isEmpty()) {
            //on startup
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.getIFragmentGenerator().generateInitialFragmentsAndStorageUnits(pathList, new TimeInterval(0, Long.MAX_VALUE));
            metaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            fragments = metaManager.getFragmentMapByTimeSeriesInterval(interval);
        }

        List<Delete> deleteList = new ArrayList<>();
        fragments.forEach((k, v) -> v.forEach(fragmentMeta -> {
            TimeInterval timeInterval = fragmentMeta.getTimeInterval();
            if (statement.isDeleteAll()) {
                deleteList.add(new Delete(new FragmentSource(fragmentMeta), null, pathList));
            } else {
                List<TimeRange> overlapTimeRange = getOverlapTimeRange(timeInterval, statement.getTimeRanges());
                if (!overlapTimeRange.isEmpty()) {
                    deleteList.add(new Delete(new FragmentSource(fragmentMeta), overlapTimeRange, pathList));
                }
            }
        }));

        List<Source> sources = new ArrayList<>();
        deleteList.forEach(operator -> sources.add(new OperatorSource(operator)));
        return new CombineNonQuery(sources);
    }

    private List<TimeRange> getOverlapTimeRange(TimeInterval interval, List<TimeRange> timeRanges) {
        List<TimeRange> res = new ArrayList<>();
        for (TimeRange range : timeRanges) {
            if (interval.getStartTime() > range.getEndTime() ||
                    interval.getEndTime() < range.getBeginTime())
                continue;
            res.add(range);
        }
        return res;
    }
}
