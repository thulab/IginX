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
package cn.edu.tsinghua.iginx.query;

import cn.edu.tsinghua.iginx.metadata.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.plan.*;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.query.result.*;

public interface IStorageEngine {

    NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan);

    NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan);

    QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan);

    NonDataPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan);

    NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan);

    NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan);

    NonDataPlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan);

    NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan);

    AvgAggregateQueryPlanExecuteResult syncExecuteAvgQueryPlan(AvgQueryPlan plan);

    StatisticsAggregateQueryPlanExecuteResult syncExecuteCountQueryPlan(CountQueryPlan plan);

    StatisticsAggregateQueryPlanExecuteResult syncExecuteSumQueryPlan(SumQueryPlan plan);

    SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstQueryPlan(FirstQueryPlan plan);

    SingleValueAggregateQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan);

    SingleValueAggregateQueryPlanExecuteResult syncExecuteMaxQueryPlan(MaxQueryPlan plan);

    SingleValueAggregateQueryPlanExecuteResult syncExecuteMinQueryPlan(MinQueryPlan plan);

    DownsampleQueryPlanExecuteResult syncExecuteDownsampleAvgQueryPlan(DownsampleAvgQueryPlan plan);

    DownsampleQueryPlanExecuteResult syncExecuteDownsampleCountQueryPlan(DownsampleCountQueryPlan plan);

    DownsampleQueryPlanExecuteResult syncExecuteDownsampleSumQueryPlan(DownsampleSumQueryPlan plan);

    DownsampleQueryPlanExecuteResult syncExecuteDownsampleMaxQueryPlan(DownsampleMaxQueryPlan plan);

    DownsampleQueryPlanExecuteResult syncExecuteDownsampleMinQueryPlan(DownsampleMinQueryPlan plan);

    DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstQueryPlan(DownsampleFirstQueryPlan plan);

    DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastQueryPlan(DownsampleLastQueryPlan plan);


    StorageEngineChangeHook getStorageEngineChangeHook();

}
