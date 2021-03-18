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
import cn.edu.tsinghua.iginx.plan.DataPlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.NonDatabasePlan;
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

    @Override
    public List<SplitInfo> getSplitResults(NonDatabasePlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap;

        if (plan.getIginxPlanType() == IginxPlan.IginxPlanType.ADD_COLUMNS) {
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
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
        } else if (plan.getIginxPlanType() == IginxPlan.IginxPlanType.DELETE_COLUMNS) {
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
            for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
                for (FragmentMeta fragment : entry.getValue()) {
                    List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false);
                    for (FragmentReplicaMeta replica : replicas) {
                        infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
                    }
                }
            }
        } else {
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                    plan.getTsInterval(), ((DataPlan) plan).getTimeInterval());
            if (fragmentMap.isEmpty()) {
                fragmentMap = iMetaManager.generateFragmentMap(plan.getStartPath(), ((DataPlan) plan).getStartTime());
                iMetaManager.tryCreateInitialFragments(fragmentMap.values().stream().flatMap(List::stream).collect(Collectors.toList()));
                fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                        plan.getTsInterval(), ((DataPlan) plan).getTimeInterval());
            }
            for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
                for (FragmentMeta fragment : entry.getValue()) {
                    List<FragmentReplicaMeta> replicas = new ArrayList<>();
                    if (plan.getIginxPlanType() == IginxPlan.IginxPlanType.INSERT_RECORDS) {
                        replicas = chooseFragmentReplicas(fragment, false);
                    } else if (plan.getIginxPlanType() == IginxPlan.IginxPlanType.QUERY_DATA) {
                        replicas = chooseFragmentReplicas(fragment, true);
                    }
                    for (FragmentReplicaMeta replica : replicas) {
                        infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), replica));
                    }
                }
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
