package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.plan.*;
import cn.edu.tsinghua.iginx.plan.downsample.*;
import cn.edu.tsinghua.iginx.split.SplitInfo;
import cn.edu.tsinghua.iginx.utils.HttpUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;


public class NewPlanSplitter implements IPlanSplitter {

    private static final Logger logger = LoggerFactory.getLogger(NewPlanSplitter.class);
    static boolean isFirst = true;
    private final IMetaManager iMetaManager;
    private final NewPolicy policy;
    private Map<String, Double> prefixList = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private int prefixMaxSize;
    private static final String UPDATE_META_URL = "/receive_meta";
    private static final String FRAGMENT_URL = "/fragment";
    private int k;

    private static final FragmentCreator fragmentCreator = FragmentCreator.getInstance();
    private static final Config config = ConfigDescriptor.getInstance().getConfig();


    private final Random random = new Random();


    public NewPlanSplitter(NewPolicy policy, IMetaManager iMetaManager) {
        this.policy = policy;
        this.iMetaManager = iMetaManager;
        this.prefixMaxSize = config.getPathSendSize();
        this.k = config.getFragmentSplitPerEngine();
    }

    private void updatePrefix(NonDatabasePlan plan) {
        logger.info("update prefix, now size = {}", prefixList.size());
        lock.writeLock().lock();
        if (prefixMaxSize <= prefixList.size()) {

            try
            {
                iMetaManager.updatePrefix(prefixList);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            prefixMaxSize += config.getPathSendSize();
            if (isFirst) {
                isFirst = false;
                policy.setNeedReAllocate(true);
            }
        }
        lock.writeLock().unlock();
        for (String path: plan.getPaths())
        {
            double value = 1.0 / plan.getPathsNum();
            if (prefixList.containsKey(path))
            {
                value += prefixList.get(path);
            }
            prefixList.put(path, value);
        }
    }

    public List<SplitInfo> getSplitAddColumnsPlanResults(AddColumnsPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
        if (fragmentMap.isEmpty()) {
            Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> fragmentsAndStorageUnits = iMetaManager.generateInitialFragmentsAndStorageUnits(plan.getPaths(), new TimeInterval(0, Long.MAX_VALUE));
            iMetaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    logger.info("add storage unit id {} to duplicate remove set.", storageUnit.getId());
                    infoList.add(new SplitInfo(new TimeInterval(0L, Long.MAX_VALUE), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    public List<SplitInfo> getSplitDeleteColumnsPlanResults(DeleteColumnsPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    logger.info("add storage unit id {} to duplicate remove set.", storageUnit.getId());
                    infoList.add(new SplitInfo(new TimeInterval(0L, Long.MAX_VALUE), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    public List<SplitInfo> getSplitInsertColumnRecordsPlanResults(InsertColumnRecordsPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        if (fragmentMap.isEmpty()) {
            Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> fragmentsAndStorageUnits = iMetaManager.generateInitialFragmentsAndStorageUnits(plan.getPaths(), plan.getTimeInterval());
            iMetaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
            policy.setNeedReAllocate(false);
        } else if (policy.isNeedReAllocate()) {
            logger.info("ReAllocate, now size = {}", prefixList.size());
            lock.writeLock().lock();
            /*
            if (policy.isNeedReAllocate())
            {
                logger.info("real ReAllocate, now size = {}", prefixList.size());
                List<String> ips = new ArrayList<>();
                for (IginxMeta iginxMeta: iMetaManager.getIginxList())
                    ips.add(iginxMeta.getIp());

                List<String> ins = new ArrayList<>();
                ins.add(String.valueOf(iMetaManager.getStorageEngineList().size() * k));
                ins.add(String.valueOf(plan.getEndTime() + 1));
                for (String ip: ips)
                {
                    String url = "http://" + ip + ":" + config.getRestPort()
                            + FRAGMENT_URL;
                    HttpUtils.doPost(url, ins);
                }
            }*/
            policy.setNeedReAllocate(false);
            lock.writeLock().unlock();
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitInsertRowRecordsPlanResults(InsertRowRecordsPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        if (fragmentMap.isEmpty()) {
            Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> fragmentsAndStorageUnits = iMetaManager.generateInitialFragmentsAndStorageUnits(plan.getPaths(), plan.getTimeInterval());
            iMetaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
            policy.setNeedReAllocate(false);
        } else if (policy.isNeedReAllocate()) {
            logger.info("ReAllocate, now size = {}", prefixList.size());
            lock.writeLock().lock();
            /*
            if (policy.isNeedReAllocate())
            {
                logger.info("real ReAllocate, now size = {}", prefixList.size());
                List<String> ips = new ArrayList<>();
                for (IginxMeta iginxMeta: iMetaManager.getIginxList())
                    ips.add(iginxMeta.getIp());

                List<String> ins = new ArrayList<>();
                ins.add(String.valueOf(iMetaManager.getStorageEngineList().size() * k));
                ins.add(String.valueOf(plan.getEndTime() + 1));
                for (String ip: ips)
                {
                    String url = "http://" + ip + ":" + config.getRestPort()
                            + FRAGMENT_URL;
                    HttpUtils.doPost(url, ins);
                }
            }*/
            policy.setNeedReAllocate(false);
            lock.writeLock().unlock();
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDeleteDataInColumnsPlanResults(DeleteDataInColumnsPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    public List<SplitInfo> getSplitQueryDataPlanResults(QueryDataPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    private List<SplitInfo> getSplitResultsForDownsamplePlan(DownsampleQueryPlan plan, IginxPlan.IginxPlanType intervalQueryPlan) {
        updatePrefix(plan);
        // 初始化结果集
        List<SplitInfo> infoList = new ArrayList<>();
        // 对查出来的分片结果按照开始时间进行排序、分组
        List<List<FragmentMeta>> fragmentMetasList = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval()).values().stream().flatMap(List::stream)
                .sorted(Comparator.comparingLong(e -> e.getTimeInterval().getStartTime()))
                .collect(Collectors.groupingBy(e -> e.getTimeInterval().getStartTime())).entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry::getKey)).map(Map.Entry::getValue).collect(Collectors.toList());
        // 聚合精度，后续会多次用到
        long precision = plan.getPrecision();
        // 计算子查询时间片列表
        List<TimeInterval> planTimeIntervals = splitTimeIntervalForDownsampleQuery(fragmentMetasList.stream()
                .map(e -> e.get(0).getTimeInterval()).collect(Collectors.toList()), plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
        // 所属的合并组的组号
        int combineGroup = 0;
        int index = 0;
        long timespan = 0L;
        for (List<FragmentMeta> fragmentMetas : fragmentMetasList) {
            long endTime = fragmentMetas.get(0).getTimeInterval().getEndTime();
            while (index < planTimeIntervals.size() && planTimeIntervals.get(index).getEndTime() <= endTime) {
                TimeInterval timeInterval = planTimeIntervals.get(index++);
                if (timeInterval.getSpan() >= precision) {
                    // 对于聚合子查询，清空 timespan，并且在计划全部加入之后增加组号
                    for (FragmentMeta fragment : fragmentMetas) {
                        List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                        for (StorageUnitMeta storageUnit : storageUnitList) {
                            infoList.add(new SplitInfo(timeInterval, fragment.getTsInterval(), storageUnit, plan.getIginxPlanType(), combineGroup));
                        }
                    }
                    timespan = 0L;
                    combineGroup += 1;
                } else {
                    for (FragmentMeta fragment : fragmentMetas) {
                        List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                        for (StorageUnitMeta storageUnit : storageUnitList) {
                            infoList.add(new SplitInfo(timeInterval, fragment.getTsInterval(), storageUnit, intervalQueryPlan, combineGroup));
                        }
                    }
                    timespan += timeInterval.getSpan();
                    if (timespan >= precision) {
                        timespan = 0L;
                        combineGroup += 1;
                    }
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleMaxQueryPlanResults(DownsampleMaxQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.MAX);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleMinQueryPlanResults(DownsampleMinQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.MIN);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleSumQueryPlanResults(DownsampleSumQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.SUM);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleCountQueryPlanResults(DownsampleCountQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.COUNT);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleAvgQueryPlanResults(DownsampleAvgQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.AVG);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleFirstQueryPlanResults(DownsampleFirstQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.FIRST);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleLastQueryPlanResults(DownsampleLastQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.LAST);
    }

    @Override
    public List<SplitInfo> getValueFilterQueryPlanResults(ValueFilterQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitMaxQueryPlanResults(MaxQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitMinQueryPlanResults(MinQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitSumQueryPlanResults(SumQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitCountQueryPlanResults(CountQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitAvgQueryPlanResults(AvgQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitFirstQueryPlanResults(FirstQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        for (String path : plan.getPaths()) {
            List<FragmentMeta> fragmentList = iMetaManager.getFragmentListByTimeSeriesNameAndTimeInterval(path, plan.getTimeInterval());
            for (FragmentMeta fragment : fragmentList) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), new TimeSeriesInterval(path, path, true), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitLastQueryPlanResults(LastQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        for (String path : plan.getPaths()) {
            List<FragmentMeta> fragmentList = iMetaManager.getFragmentListByTimeSeriesNameAndTimeInterval(path, plan.getTimeInterval());
            for (FragmentMeta fragment : fragmentList) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), new TimeSeriesInterval(path, path, true), storageUnit));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<Long> getSplitShowColumnsPlanResult() {
        return iMetaManager.getStorageEngineList().stream().map(StorageEngineMeta::getId).collect(Collectors.toList());
    }

    @Override
    public List<StorageUnitMeta> selectStorageUnitList(FragmentMeta fragment, boolean isQuery) {
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();
        // TODO 暂时设置为只查主
        storageUnitList.add(fragment.getMasterStorageUnit());
        if (!isQuery) {
            storageUnitList.addAll(fragment.getMasterStorageUnit().getReplicas());
        }
        return storageUnitList;
    }

    public static List<TimeInterval> splitTimeIntervalForDownsampleQuery(List<TimeInterval> timeIntervals,
                                                                         long beginTime, long endTime, long precision) {
        List<TimeInterval> resultList = new ArrayList<>();
        for (TimeInterval timeInterval : timeIntervals) {
            long midIntervalBeginTime = Math.max(timeInterval.getStartTime(), beginTime);
            long midIntervalEndTime = Math.min(timeInterval.getEndTime(), endTime);
            if (timeInterval.getStartTime() > beginTime && (timeInterval.getStartTime() - beginTime) % precision != 0) { // 只有非第一个分片才有可能创建前缀分片
                long prefixIntervalEndTime = Math.min(midIntervalBeginTime + precision - (timeInterval.getStartTime() - beginTime) % precision, midIntervalEndTime);
                resultList.add(new TimeInterval(midIntervalBeginTime, prefixIntervalEndTime));
                midIntervalBeginTime = prefixIntervalEndTime;
            }
            if ((midIntervalEndTime - midIntervalBeginTime) >= precision) {
                midIntervalEndTime -= (midIntervalEndTime - midIntervalBeginTime) % precision;
                resultList.add(new TimeInterval(midIntervalBeginTime, midIntervalEndTime));
            } else {
                midIntervalEndTime = midIntervalBeginTime;
            }
            if (midIntervalEndTime != Math.min(timeInterval.getEndTime(), endTime)) {
                resultList.add(new TimeInterval(midIntervalEndTime, Math.min(timeInterval.getEndTime(), endTime)));
            }
        }
        return resultList;
    }
}