package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.NonDatabasePlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.ValueFilterQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.split.SplitInfo;
import cn.edu.tsinghua.iginx.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;


public class NewPlanSplitter implements IPlanSplitter {

    private static final Logger logger = LoggerFactory.getLogger(NewPlanSplitter.class);
    static boolean isFirst = true;
    private final IMetaManager iMetaManager;
    private final NewPolicy policy;
    private final Set<String> prefixSet = new HashSet<>();
    private List<String> prefixList = new LinkedList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private int prefixMaxSize;
    private static final String UPDATE_META_URL = "/receive_meta";
    private static final String FRAGMENT_URL = "/fragment";
    private int k;

    private static final Config config = ConfigDescriptor.getInstance().getConfig();


    private final Random random = new Random();


    public NewPlanSplitter(NewPolicy policy, IMetaManager iMetaManager) {
        this.policy = policy;
        this.iMetaManager = iMetaManager;
        this.prefixMaxSize = 1760;
        this.k = config.getFragmentSplitPerEngine();
    }

    private void updatePrefix(NonDatabasePlan plan) {
        lock.writeLock().lock();
        logger.info("update prefix, now size = {}", prefixSet.size());
        if (prefixMaxSize <= prefixSet.size()) {
            String url = "http://" + config.getNewPolicyRestIp() + ":" + config.getNewPolicyRestPort()
                    + UPDATE_META_URL;
            HttpUtils.doPost(url, prefixList);
            prefixMaxSize *= 2;
            prefixList = new ArrayList<>();
            if (isFirst) {
                isFirst = false;
                policy.setNeedReAllocate(true);
            }
        }
        for (String path: plan.getPaths())
        {
            if (!prefixSet.contains(path))
            {
                if (prefixSet.size() == prefixMaxSize)
                {
                    int tmp = random.nextInt(prefixMaxSize);
                    prefixSet.remove(prefixList.get(tmp));
                    prefixList.remove(tmp);
                }
                prefixSet.add(path);
                prefixList.add(path);
            }
        }
        lock.writeLock().unlock();
    }

    public List<SplitInfo> getSplitAddColumnsPlanResults(AddColumnsPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
        if (fragmentMap.isEmpty()) {
            fragmentMap = iMetaManager.generateFragments(plan.getStartPath(), 0L);
            iMetaManager.tryCreateInitialFragments(fragmentMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false);
                Set<Long> storageEngineIds = new HashSet<>();
                for (FragmentReplicaMeta replica : replicas) {
                    if (storageEngineIds.contains(replica.getStorageEngineId())) {
                        logger.info("storage engine id " + replica.getStorageEngineId() + " is duplicated.");
                        continue;
                    }
                    storageEngineIds.add(replica.getStorageEngineId());
                    logger.info("add storage engine id " + replica.getStorageEngineId() + " to duplicate remove set.");
                    infoList.add(new SplitInfo(new TimeInterval(0L, Long.MAX_VALUE), entry.getKey(), replica));
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
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false);
                Set<Long> storageEngineIds = new HashSet<>();
                for (FragmentReplicaMeta replica : replicas) {
                    if (storageEngineIds.contains(replica.getStorageEngineId())) {
                        logger.info("storage engine id " + replica.getStorageEngineId() + " is duplicated.");
                        continue;
                    }
                    storageEngineIds.add(replica.getStorageEngineId());
                    logger.info("add storage engine id " + replica.getStorageEngineId() + " to duplicate remove set.");
                    infoList.add(new SplitInfo(new TimeInterval(0L, Long.MAX_VALUE), entry.getKey(), replica));
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
            fragmentMap = iMetaManager.generateFragments(plan.getStartPath(), plan.getStartTime());
            iMetaManager.tryCreateInitialFragments(fragmentMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
            policy.setNeedReAllocate(false);
        } else if (policy.isNeedReAllocate()) {
            logger.info("ReAllocate, now size = {}", prefixSet.size());
            lock.writeLock().lock();
            if (policy.isNeedReAllocate())
            {
                logger.info("real ReAllocate, now size = {}", prefixSet.size());
                String url = "http://" + config.getNewPolicyRestIp() + ":" + config.getNewPolicyRestPort()
                        + FRAGMENT_URL;
                List<String> ins = new ArrayList<>();
                ins.add(String.valueOf(iMetaManager.getStorageEngineList().size() * k));
                ins.add(String.valueOf(plan.getEndTime() + 1));
                HttpUtils.doPost(url, ins);
                policy.setNeedReAllocate(false);
            }
            lock.writeLock().unlock();
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
            fragmentMap = iMetaManager.generateFragments(plan.getStartPath(), plan.getStartTime());
            iMetaManager.tryCreateInitialFragments(fragmentMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
            policy.setNeedReAllocate(false);
        } else if (policy.isNeedReAllocate()) {
            logger.info("ReAllocate, now size = {}", prefixSet.size());
            lock.writeLock().lock();
            if (policy.isNeedReAllocate())
            {
                logger.info("real ReAllocate, now size = {}", prefixSet.size());
                String url = "http://" + config.getNewPolicyRestIp() + ":" + config.getNewPolicyRestPort()
                        + FRAGMENT_URL;
                List<String> ins = new ArrayList<>();
                ins.add(String.valueOf(iMetaManager.getStorageEngineList().size() * k));
                ins.add(String.valueOf(plan.getEndTime() + 1));
                HttpUtils.doPost(url, ins);
                policy.setNeedReAllocate(false);
            }
            lock.writeLock().unlock();
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleMaxQueryPlanResults(DownsampleMaxQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleMinQueryPlanResults(DownsampleMinQueryPlan plan) {
        return null;
    }

    @Override
    public List<SplitInfo> getSplitSumQueryPlanResults(SumQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleSumQueryPlanResults(DownsampleSumQueryPlan plan) {
        return null;
    }

    @Override
    public List<SplitInfo> getSplitCountQueryPlanResults(CountQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleCountQueryPlanResults(DownsampleCountQueryPlan plan) {
        return null;
    }

    @Override
    public List<SplitInfo> getSplitAvgQueryPlanResults(AvgQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleAvgQueryPlanResults(DownsampleAvgQueryPlan plan) {
        return null;
    }

    @Override
    public List<SplitInfo> getSplitFirstQueryPlanResults(FirstQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        for (String path : plan.getPaths()) {
            List<FragmentMeta> fragmentList = iMetaManager.getFragmentListByTimeSeriesNameAndTimeInterval(path, plan.getTimeInterval());
            FragmentMeta fragment = fragmentList.get(0);
            for (FragmentReplicaMeta replica : chooseFragmentReplicas(fragment, true)) {
                infoList.add(new SplitInfo(fragment.getTimeInterval(), new TimeSeriesInterval(path, path), replica));
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleFirstQueryPlanResults(DownsampleFirstQueryPlan plan) {
        return null;
    }

    @Override
    public List<SplitInfo> getSplitLastQueryPlanResults(LastQueryPlan plan) {
        updatePrefix(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        for (String path : plan.getPaths()) {
            List<FragmentMeta> fragmentList = iMetaManager.getFragmentListByTimeSeriesNameAndTimeInterval(path, plan.getTimeInterval());
            FragmentMeta fragment = fragmentList.get(fragmentList.size() - 1);
            for (FragmentReplicaMeta replica : chooseFragmentReplicas(fragment, true)) {
                infoList.add(new SplitInfo(fragment.getTimeInterval(), new TimeSeriesInterval(path, path), replica));
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleLastQueryPlanResults(DownsampleLastQueryPlan plan) {
        return null;
    }

    @Override
    public List<SplitInfo> getValueFilterQueryPlanResults(ValueFilterQueryPlan plan) {
        return null;
    }

    @Override
    public List<FragmentReplicaMeta> selectFragmentReplicas(FragmentMeta fragment, boolean isQuery) {
        List<FragmentReplicaMeta> replicas = new ArrayList<>();
        if (isQuery) {
            replicas.add(fragment.getReplicaMetas().get(0));
        } else {
            replicas.addAll(fragment.getReplicaMetas().values());
        }
        return replicas;
    }

    public List<FragmentReplicaMeta> chooseFragmentReplicas(FragmentMeta fragment, boolean isQuery) {
        List<FragmentReplicaMeta> replicas = new ArrayList<>();
        if (isQuery) {
            replicas.add(fragment.getReplicaMetas().get(0));
        } else {
            replicas.addAll(fragment.getReplicaMetas().values());
        }
        return replicas;
    }
}