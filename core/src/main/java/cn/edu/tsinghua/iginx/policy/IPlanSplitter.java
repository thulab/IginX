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

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.split.SplitInfo;

import java.util.List;

public interface IPlanSplitter {

	/**
	 * 拆分 AddColumnsPlan
	 * @param plan 待拆分的 AddColumnsPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitAddColumnsPlanResults(AddColumnsPlan plan);

	/**
	 * 拆分 DeleteColumnsPlan
	 * @param plan 待拆分的 DeleteColumnsPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitDeleteColumnsPlanResults(DeleteColumnsPlan plan);

	/**
	 * 拆分 InsertRecordsPlan
	 * @param plan 待拆分的 InsertRecordsPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitInsertRecordsPlanResults(InsertRecordsPlan plan);

	/**
	 * 拆分 DeleteDataInColumnsPlan
	 * @param plan 待拆分的 DeleteDataInColumnsPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitDeleteDataInColumnsPlanResults(DeleteDataInColumnsPlan plan);

	/**
	 * 拆分 QueryDataPlan
	 * @param plan 待拆分的 QueryDataPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitQueryDataPlanResults(QueryDataPlan plan);

	/**
	 * 拆分 MaxQueryPlan
	 * @param plan 待拆分的 MaxQueryPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitMaxQueryPlanResults(MaxQueryPlan plan);

	/**
	 * 拆分 MinQueryPlan
	 * @param plan 待拆分的 MinQueryPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitMinQueryPlanResults(MinQueryPlan plan);

	/**
	 * 拆分 SumQueryPlan
	 * @param plan 待拆分的 SumQueryPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitSumQueryPlanResults(SumQueryPlan plan);

	/**
	 * 拆分 CountQueryPlan
	 * @param plan 待拆分的 CountQueryPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitCountQueryPlanResults(CountQueryPlan plan);

	/**
	 * 拆分 AvgQueryPlan
	 * @param plan 待拆分的 AvgQueryPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitAvgQueryPlanResults(AvgQueryPlan plan);

	/**
	 * 拆分 FirstQueryPlan
	 * @param plan 待拆分的 FirstQueryPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitFirstQueryPlanResults(FirstQueryPlan plan);

	/**
	 * 拆分 LastQueryPlan
	 * @param plan 待拆分的 LastQueryPlan
	 * @return 拆分方式
	 */
	List<SplitInfo> getSplitLastQueryPlanResults(LastQueryPlan plan);

	/**
	 * 从给定的分片中选择副本
	 * @param fragment 被选择的分片
	 * @param isQuery 是否为查询计划选取副本
	 * @return 选出的分片副本
	 */
	List<FragmentReplicaMeta> chooseFragmentReplicas(FragmentMeta fragment, boolean isQuery);
}
