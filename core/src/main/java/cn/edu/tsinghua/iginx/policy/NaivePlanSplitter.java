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

import cn.edu.tsinghua.iginx.metadatav2.IMetaManager;
import cn.edu.tsinghua.iginx.metadatav2.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.split.SplitInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class NaivePlanSplitter implements IPlanSplitter {

    private final IMetaManager iMetaManager;

    public NaivePlanSplitter(IMetaManager iMetaManager) {
        this.iMetaManager = iMetaManager;
    }

    public List<SplitInfo> getSplitAddColumnsPlanResults(AddColumnsPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
        if (fragmentMap.isEmpty()) {
            fragmentMap = iMetaManager.generateFragmentMap(plan.getStartPath(), 0L);
            iMetaManager.tryCreateInitialFragments(fragmentMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false);
                for (FragmentReplicaMeta replica : replicas) {
                    infoList.add(new SplitInfo(new TimeInterval(0L, Long.MAX_VALUE), entry.getKey(), replica));
                }
            }
        }
        return infoList;
    }

    public List<SplitInfo> getSplitDeleteColumnsPlanResults(DeleteColumnsPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
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

    public List<SplitInfo> getSplitInsertRecordsPlanResults(InsertRecordsPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        if (fragmentMap.isEmpty()) {
            fragmentMap = iMetaManager.generateFragmentMap(plan.getStartPath(), plan.getStartTime());
            iMetaManager.tryCreateInitialFragments(fragmentMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                    plan.getTsInterval(), plan.getTimeInterval());
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

    public List<SplitInfo> getSplitQueryDataPlanResults(QueryDataPlan plan) {
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
    public List<SplitInfo> getSplitMinQueryPlanResults(MinQueryPlan plan) {
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
    public List<SplitInfo> getSplitSumQueryPlanResults(SumQueryPlan plan) {
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
    public List<SplitInfo> getSplitCountQueryPlanResults(CountQueryPlan plan) {
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
    public List<SplitInfo> getSplitAvgQueryPlanResults(AvgQueryPlan plan) {
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
    public List<SplitInfo> getSplitFirstQueryPlanResults(FirstQueryPlan plan) {
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
    public List<SplitInfo> getSplitLastQueryPlanResults(LastQueryPlan plan) {
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
    public List<FragmentReplicaMeta> chooseFragmentReplicas(FragmentMeta fragment, boolean isQuery) {
        List<FragmentReplicaMeta> replicas = new ArrayList<>();
        if (isQuery) {
            replicas.add(fragment.getReplicaMetas().get(new Random().nextInt(fragment.getReplicaMetasNum())));
        } else {
            replicas.addAll(fragment.getReplicaMetas().values());
        }
        return replicas;
    }
}
