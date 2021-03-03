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
package cn.edu.tsinghua.iginx.split;

import cn.edu.tsinghua.iginx.metadata.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.plan.DataPlan;

import java.util.List;

public interface IPlanSplitter {

	/**
	 * 拆分 DataPlan
	 * @param plan 待拆分的 DataPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitResults(DataPlan plan);

	/**
	 * 从给定的分片中选择副本
	 * @param fragment 被选择的分片
	 * @param isQuery 是否为查询计划选取副本
	 * @param replicaNum 待选择的副本数量
	 * @return 选出的分片副本
	 */
	List<FragmentReplicaMeta> chooseFragmentReplicas(FragmentMeta fragment, boolean isQuery, int replicaNum);
}
