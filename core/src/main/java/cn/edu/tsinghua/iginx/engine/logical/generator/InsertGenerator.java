package cn.edu.tsinghua.iginx.engine.logical.generator;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.CombineNonQuery;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
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
import cn.edu.tsinghua.iginx.engine.logical.optimizer.Optimizer;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SortUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class InsertGenerator implements LogicalGenerator {

    private final GeneratorType type = GeneratorType.Insert;

    private static final Logger logger = LoggerFactory.getLogger(InsertGenerator.class);

    private final static InsertGenerator instance = new InsertGenerator();

    private final List<Optimizer> optimizerList = new ArrayList<>();

    private final static IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final IPolicy policy = PolicyManager.getInstance()
            .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

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
        policy.notify(statement);

        List<String> pathList = SortUtils.mergeAndSortPaths(new ArrayList<>(statement.getPaths()));

        TimeSeriesInterval interval = new TimeSeriesInterval(pathList.get(0), pathList.get(pathList.size() - 1));

        Map<TimeSeriesInterval, List<FragmentMeta>> fragments = metaManager.getFragmentMapByTimeSeriesInterval(interval);
        if (fragments.isEmpty()) {
            //on startup
            policy.setNeedReAllocate(false);
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.generateInitialFragmentsAndStorageUnits(statement);
            metaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            fragments = metaManager.getFragmentMapByTimeSeriesInterval(interval);
        } else if (policy.isNeedReAllocate()) {
            //on scale-out or any events requiring reallocation
            logger.debug("Trig ReAllocate!");
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.generateFragmentsAndStorageUnits(statement);
            metaManager.createFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
        }

        RawData rawData = statement.getRawData();
        List<Insert> insertList = new ArrayList<>();
        fragments.forEach((k, v) -> v.forEach(fragmentMeta -> {
            DataView section = getDataSection(fragmentMeta, rawData);
            if (section != null) {
                insertList.add(new Insert(new FragmentSource(fragmentMeta), section));
            }
        }));

        List<Source> sources = new ArrayList<>();
        insertList.forEach(operator -> sources.add(new OperatorSource(operator)));
        return new CombineNonQuery(sources);
    }

    private DataView getDataSection(FragmentMeta meta, RawData rawData) {
        TimeInterval timeInterval = meta.getTimeInterval();
        TimeSeriesInterval tsInterval = meta.getTsInterval();
        List<Long> insertTimes = rawData.getTimestamps();
        List<String> paths = rawData.getPaths();

        // time overlap doesn't exist.
        if (timeInterval.getStartTime() > insertTimes.get(insertTimes.size() - 1) ||
                timeInterval.getEndTime() < insertTimes.get(0)) {
            return null;
        }

        // path overlap doesn't exist.
        if (tsInterval.getStartTimeSeries() != null &&
                tsInterval.getStartTimeSeries().compareTo(paths.get(paths.size() - 1)) > 0)
            return null;
        if (tsInterval.getEndTimeSeries() != null &&
                tsInterval.getEndTimeSeries().compareTo(paths.get(0)) < 0) {
            return null;
        }

        int startTimeIndex = 0;
        while (timeInterval.getStartTime() > insertTimes.get(startTimeIndex))
            startTimeIndex++;
        int endTimeIndex = startTimeIndex;
        while (endTimeIndex < insertTimes.size() && timeInterval.getEndTime() > insertTimes.get(endTimeIndex))
            endTimeIndex++;


        int startPathIndex = 0;
        if (tsInterval.getStartTimeSeries() != null) {
            while (tsInterval.getStartTimeSeries().compareTo(paths.get(startPathIndex)) > 0)
                startPathIndex++;
        }
        int endPathIndex = startPathIndex;
        if (tsInterval.getEndTimeSeries() != null) {
            while (endPathIndex < paths.size() && tsInterval.getEndTimeSeries().compareTo(paths.get(endPathIndex)) > 0)
                endPathIndex++;
        } else {
            endPathIndex = paths.size();
        }

        if (rawData.isRowData()) {
            return new RowDataView(rawData, startPathIndex, endPathIndex, startTimeIndex, endTimeIndex);
        } else {
            return new ColumnDataView(rawData, startPathIndex, endPathIndex, startTimeIndex, endTimeIndex);
        }
    }
}
