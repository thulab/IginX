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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.MetaManager;
import cn.edu.tsinghua.iginx.plan.DataPlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.NonDatabasePlan;
import cn.edu.tsinghua.iginx.split.SplitInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class NaivePlanSplitter implements IPlanSplitter {

    private final IMetaManager iMetaManager;

    public NaivePlanSplitter(IMetaManager iMetaManager) {
        this.iMetaManager = iMetaManager;
    }

    @Override
    public List<SplitInfo> getSplitResults(NonDatabasePlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        List<FragmentMeta> fragments;

        for (Map.Entry<String, List<Integer>> entry : plan.generateIndexesOfPaths().entrySet()) {
            if (plan.getIginxPlanType() == IginxPlan.IginxPlanType.ADD_COLUMNS) {
                fragments = iMetaManager.getFragmentListByKey(entry.getKey());
                if (fragments.isEmpty()) {
                    fragments.add(createFragment(entry.getKey(), 0L, Long.MAX_VALUE));
                }
                for (FragmentMeta fragment : fragments) {
                    List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false, ConfigDescriptor.getInstance().getConfig().getReplicaNum());
                    for (FragmentReplicaMeta replica : replicas) {
                        infoList.add(new SplitInfo(entry.getValue(), replica));
                    }
                }
            } else if (plan.getIginxPlanType() == IginxPlan.IginxPlanType.DELETE_COLUMNS) {
                fragments = iMetaManager.getFragmentListByKey(entry.getKey());
                for (FragmentMeta fragment : fragments) {
                    List<FragmentReplicaMeta> replicas = chooseFragmentReplicas(fragment, false, ConfigDescriptor.getInstance().getConfig().getReplicaNum());
                    for (FragmentReplicaMeta replica : replicas) {
                        infoList.add(new SplitInfo(entry.getValue(), replica));
                    }
                }
            } else {
                fragments = iMetaManager.getFragmentListByKeyAndTimeInterval(
                        entry.getKey(), ((DataPlan) plan).getStartTime(), ((DataPlan) plan).getEndTime());
                if (fragments.isEmpty()) {
                    fragments.add(createFragment(entry.getKey(), 0L, Long.MAX_VALUE));
                }
                for (FragmentMeta fragment : fragments) {
                    List<FragmentReplicaMeta> replicas = new ArrayList<>();
                    if (plan.getIginxPlanType() == IginxPlan.IginxPlanType.INSERT_RECORDS) {
                        replicas = chooseFragmentReplicas(fragment, false, ConfigDescriptor.getInstance().getConfig().getReplicaNum());
                    } else if (plan.getIginxPlanType() == IginxPlan.IginxPlanType.QUERY_DATA) {
                        replicas = chooseFragmentReplicas(fragment, true, 0);
                    }
                    for (FragmentReplicaMeta replica : replicas) {
                        infoList.add(new SplitInfo(entry.getValue(), replica));
                    }
                }
            }
        }

        return infoList;
    }

    @Override
    public List<FragmentReplicaMeta> chooseFragmentReplicas(FragmentMeta fragment, boolean isQuery, int replicaNum) {
        List<FragmentReplicaMeta> replicas = new ArrayList<>();
        Random random = new Random();
        if (isQuery) {
            replicas.add(fragment.getReplicaMetas().get(random.nextInt(fragment.getReplicaMetasNum())));
        } else {
            replicas.addAll(fragment.getReplicaMetas().values());
        }
        return replicas;
    }

    public static FragmentMeta createFragment(String key, long startTime, long endTime) {
        List<Long> databaseIds = MetaManager.getInstance().chooseDatabaseIdsForNewFragment();
        FragmentMeta fragment = new FragmentMeta(key, startTime, endTime, databaseIds);
        MetaManager.getInstance().createFragment(fragment);
        return fragment;
    }
}
