package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowTimeSeries;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.naive.NaivePolicy;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.*;

public class MigrationExample {

    private final IPolicy policy = new NaivePolicy();
    private final MigrationPolicy migrationPolicy = new GreedyMigrationPolicy();
    private List<FragmentMeta> fragmentMetas;
    private List<StorageUnitMeta> storageUnitMetas;
    private final PhysicalEngine physicalEngine = PhysicalEngineImpl.getInstance();

    public MigrationExample() {
        // 建立初始分片
        List<String> paths = new ArrayList<>();
        paths.add("a.b.c");
        paths.add("b.c.a");
        paths.add("c.a.b");
        paths.add("d.a.b");
        paths.add("e.a.b");
        paths.add("f.a.b");
        List<Long> times = new ArrayList<>();
        times.add(1000L);
        Object[] values = new Object[3];
        values[0] = 1;
        values[1] = 2;
        values[2] = 3;
        List<DataType> types = new ArrayList<>();
        types.add(DataType.INTEGER);
        types.add(DataType.INTEGER);
        types.add(DataType.INTEGER);
        InsertStatement insertStatement = new InsertStatement(
                RawDataType.Column,
                paths,
                times,
                values,
                types,
                new ArrayList<>(),
                null
        );
        policy.init(DefaultMetaManager.getInstance());
        Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.generateInitialFragmentsAndStorageUnits(insertStatement);
        this.fragmentMetas = fragmentsAndStorageUnits.k;
        this.storageUnitMetas = fragmentsAndStorageUnits.v;
        DefaultMetaManager.getInstance().createInitialFragmentsAndStorageUnits(storageUnitMetas, fragmentMetas);
    }

    /**
     * 在时间序列层面将分片在同一个du下分为两块（未知时间序列, 写入场景，根据查询出来的相关时间序列一分为二）
     */
    public void executeReshardWriteByTimeseries() throws PhysicalException {
        migrationPolicy.reshardWriteByTimeseries(this.fragmentMetas.get(0), 0);//points是未来合入的负载均衡策略统计用，直接使用迁移接口时传入0即可
        Map<TimeSeriesInterval, List<FragmentMeta>> allTimeseriesFragments = DefaultMetaManager.getInstance().getFragmentMapByTimeSeriesInterval(new TimeSeriesInterval("a.a.a", null));
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> timeseriesFragmentsEntry : allTimeseriesFragments.entrySet()) {
            System.out.println("timeseries interval: " + timeseriesFragmentsEntry.getKey().toString());
            for (FragmentMeta fragmentMeta : timeseriesFragmentsEntry.getValue()) {
                System.out.println("fragment: " + fragmentMeta.toString());
            }
        }
    }

    /**
     * 在时间序列层面将分片在同一个du下分为两块（已知时间序列，根据传入的时间序列load一分为二）
     */
    public void executeReshardQueryByTimeseries() {
        Map<String, Long> timeseriesLoadMap = new HashMap<>();
        timeseriesLoadMap.put("a.b.c", 1L);
        timeseriesLoadMap.put("b.c.a", 1L);
        timeseriesLoadMap.put("c.a.b", 1L);
        timeseriesLoadMap.put("d.a.b", 1L);
        timeseriesLoadMap.put("e.a.b", 1L);
        timeseriesLoadMap.put("f.a.b", 1L);
        migrationPolicy.reshardQueryByTimeseries(this.fragmentMetas.get(0), timeseriesLoadMap);//timeseriesLoadMap表示要进行分片的分片中各时间序列的load
        Map<TimeSeriesInterval, List<FragmentMeta>> allTimeseriesFragments = DefaultMetaManager.getInstance().getFragmentMapByTimeSeriesInterval(new TimeSeriesInterval("a.a.a", null));
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> timeseriesFragmentsEntry : allTimeseriesFragments.entrySet()) {
            System.out.println("timeseries interval: " + timeseriesFragmentsEntry.getKey().toString());
            for (FragmentMeta fragmentMeta : timeseriesFragmentsEntry.getValue()) {
                System.out.println("fragment: " + fragmentMeta.toString());
            }
        }
    }

    /**
     * 分片迁移（带数据）
     */
    public void executeMigrateData() {
        //截断正在写入的分片，只能迁移endTime不为无穷大的分片
        FragmentMeta targetFragmentMeta = fragmentMetas.get(fragmentMetas.size() - 1);
        if (fragmentMetas.get(fragmentMetas.size() - 1).getTimeInterval().getEndTime() == Long.MAX_VALUE) {
            targetFragmentMeta = migrationPolicy.reshardFragment(0, 0, targetFragmentMeta);
        }
        //迁移数据
        migrationPolicy.migrateData(0, 1, targetFragmentMeta);
        Map<TimeSeriesInterval, List<FragmentMeta>> allTimeseriesFragments = DefaultMetaManager.getInstance().getFragmentMapByTimeSeriesInterval(new TimeSeriesInterval("a.a.a", null));
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> timeseriesFragmentsEntry : allTimeseriesFragments.entrySet()) {
            System.out.println("timeseries interval: " + timeseriesFragmentsEntry.getKey().toString());
            for (FragmentMeta fragmentMeta : timeseriesFragmentsEntry.getValue()) {
                System.out.println("fragment storageId: " + fragmentMeta.getMasterStorageUnitId());
            }
        }
    }

    /**
     * 分片迁移执行
     */
    public void executeMigrateCMD() throws PhysicalException {
        //截断正在写入的分片，只能迁移endTime不为无穷大的分片
        if (fragmentMetas.get(0).getTimeInterval().getEndTime() == Long.MAX_VALUE) {
            fragmentMetas.set(0, migrationPolicy.reshardFragment(0, 0, fragmentMetas.get(0)));
        }
        Set<String> pathRegexSet = new HashSet<>();
        pathRegexSet.add(fragmentMetas.get(0).getMasterStorageUnitId());
        ShowTimeSeries showTimeSeries = new ShowTimeSeries(new GlobalSource(), pathRegexSet, null,
                Integer.MAX_VALUE, 0);
        RowStream rowStream = physicalEngine.execute(showTimeSeries);
        SortedSet<String> pathSet = new TreeSet<>();
        rowStream.getHeader().getFields().forEach(field -> {
            String timeSeries = field.getName();
            if (fragmentMetas.get(0).getTsInterval().isContain(timeSeries)) {
                pathSet.add(timeSeries);
            }
        });
        // 开始迁移数据
        Migration migration = new Migration(new GlobalSource(), 0, 1,
                fragmentMetas.get(0), new ArrayList<>(pathSet), storageUnitMetas.get(0));
        physicalEngine.execute(migration);
        // 迁移完开始删除原数据
        List<String> paths = new ArrayList<>();
        paths.add(fragmentMetas.get(0).getMasterStorageUnitId() + "*");
        List<TimeRange> timeRanges = new ArrayList<>();
        timeRanges.add(new TimeRange(fragmentMetas.get(0).getTimeInterval().getStartTime(), true,
                fragmentMetas.get(0).getTimeInterval().getEndTime(), false));
        Delete delete = new Delete(new FragmentSource(fragmentMetas.get(0)), timeRanges, paths, null);
        physicalEngine.execute(delete);
    }
}
