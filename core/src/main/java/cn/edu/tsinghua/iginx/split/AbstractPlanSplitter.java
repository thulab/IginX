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

import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractPlanSplitter {

	/**
	 * 将 InsertRecordsPlan 拆分为若干子计划
	 * @param plan 待拆分的 InsertRecordsPlan
	 * @param infoList 拆分方式
	 * @return 拆分结果
	 */
	public List<InsertRecordsPlan> splitInsertRecordsPlan(InsertRecordsPlan plan, List<SplitInfo> infoList) {
		List<InsertRecordsPlan> plans = new ArrayList<>();

		for (SplitInfo info : infoList) {
			Pair<long[], List<Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByRange(
					info.getReplica().getStartTime(), info.getReplica().getEndTime());
			Object[] values = plan.getValuesByIndexes(timestampsAndIndexes.v, info.getPathsIndexes());
			plans.add(new InsertRecordsPlan(plan.getPathsByIndexes(info.getPathsIndexes()), timestampsAndIndexes.k, values,
					plan.getAttributesByIndexes(info.getPathsIndexes()), info.getReplica().getDatabaseId()));
		}

		return plans;
	}

	/**
	 * 将 QueryDataPlan 拆分为若干子计划
	 * @param plan 待拆分的 QueryDataPlan
	 * @param infoList 拆分方式
	 * @return 拆分结果
	 */
	public List<QueryDataPlan> splitQueryDataPlan(QueryDataPlan plan, List<SplitInfo> infoList) {
		List<QueryDataPlan> plans = new ArrayList<>();

		for (SplitInfo info : infoList) {
			plans.add(new QueryDataPlan(plan.getPathsByIndexes(info.getPathsIndexes()),
					Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
					Math.min(plan.getEndTime(), info.getReplica().getEndTime()), info.getReplica().getDatabaseId()));
		}

		return plans;
	}
}
