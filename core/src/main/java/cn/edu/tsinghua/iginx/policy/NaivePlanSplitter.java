/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentReplicaMeta;
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
import cn.edu.tsinghua.iginx.split.SplitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class NaivePlanSplitter implements IPlanSplitter {

    private static final Logger logger = LoggerFactory.getLogger(NaivePlanSplitter.class);

    private final IMetaManager iMetaManager;

    private final NativePolicy policy;

    private final Set<String> prefixSet = new HashSet<>();

    private final List<String> prefixList = new LinkedList<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final int prefixMaxSize;

    private final Random random = new Random();

    public NaivePlanSplitter(NativePolicy policy, IMetaManager iMetaManager) {
        this.policy = policy;
        this.iMetaManager = iMetaManager;
        this.prefixMaxSize = 100;
    }

    private void updatePrefix(NonDatabasePlan plan) {
        lock.readLock().lock();
        if (prefixMaxSize <= prefixSet.size()) {
            lock.readLock().unlock();
            return;
        }
        lock.readLock().unlock();
        lock.writeLock().lock();
        if (prefixMaxSize <= prefixSet.size()) {
            lock.writeLock().unlock();
            return;
        }
        TimeSeriesInterval tsInterval = plan.getTsInterval();
        if (tsInterval.getStartTimeSeries() != null && !prefixSet.contains(tsInterval.getStartTimeSeries())) {
            prefixSet.add(tsInterval.getStartTimeSeries());
            prefixList.add(tsInterval.getStartTimeSeries());
        }
        if (tsInterval.getEndTimeSeries() != null && !prefixSet.contains(tsInterval.getEndTimeSeries())) {
            prefixSet.add(tsInterval.getEndTimeSeries());
            prefixList.add(tsInterval.getEndTimeSeries());
        }
        lock.writeLock().unlock();
    }

    public List<String> samplePrefix(int count) {
        lock.readLock().lock();
        String[] prefixArray = new String[prefixList.size()];
        prefixList.toArray(prefixArray);
        lock.readLock().unlock();
        for (int i = 0; i < prefixList.size(); i++) {
            int next = random.nextInt(prefixList.size());
            String value = prefixArray[next];
            prefixArray[next] = prefixArray[i];
            prefixArray[i] = value;
        }
        List<String> resultList = new ArrayList<>();
        for (int i = 0; i < count && i < prefixArray.length; i++) {
            resultList.add(prefixArray[i]);
        }
        return resultList;
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
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, false);
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
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, false);
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
            List<FragmentMeta> fragments = iMetaManager.generateFragments(samplePrefix(iMetaManager.getStorageEngineList().size() - 1), plan.getEndTime());
            iMetaManager.createFragments(fragments);
            policy.setNeedReAllocate(false);
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, false);
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
            List<FragmentMeta> fragments = iMetaManager.generateFragments(samplePrefix(iMetaManager.getStorageEngineList().size() - 1), plan.getEndTime() + 1);
            iMetaManager.createFragments(fragments);
            policy.setNeedReAllocate(false);
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, false);
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
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, false);
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
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, true);
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
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
                List<FragmentReplicaMeta> replicas = selectFragmentReplicas(fragment, true);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
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
            FragmentMeta fragment = fragmentList.get(0);
            for (FragmentReplicaMeta replica : selectFragmentReplicas(fragment, true)) {
                infoList.add(new SplitInfo(fragment.getTimeInterval(), new TimeSeriesInterval(path, path), replica));
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
            FragmentMeta fragment = fragmentList.get(fragmentList.size() - 1);
            for (FragmentReplicaMeta replica : selectFragmentReplicas(fragment, true)) {
                infoList.add(new SplitInfo(fragment.getTimeInterval(), new TimeSeriesInterval(path, path), replica));
            }
        }
        return infoList;
    }

    @Override
    public List<FragmentReplicaMeta> selectFragmentReplicas(FragmentMeta fragment, boolean isQuery) {
        List<FragmentReplicaMeta> replicas = new ArrayList<>();
        if (isQuery) {
            // TODO 暂时设置为只查主分片
            replicas.add(fragment.getReplicaMetas().get(0));
//            replicas.add(fragment.getReplicaMetas().get(new Random().nextInt(fragment.getReplicaMetasNum())));
        } else {
            replicas.addAll(fragment.getReplicaMetas().values());
        }
        return replicas;
    }
}
