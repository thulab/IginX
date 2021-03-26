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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.context.AddColumnsContext;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.CreateDatabaseContext;
import cn.edu.tsinghua.iginx.core.context.DeleteColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DeleteDataInColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DropDatabaseContext;
import cn.edu.tsinghua.iginx.core.context.InsertRecordsContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DropDatabasePlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.policy.IPlanSplitter;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.thrift.AddColumnsReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.CreateDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DropDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.InsertRecordsReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteArray;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesListByDataType;

public class SimplePlanGenerator implements IPlanGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SimplePlanGenerator.class);

    private final IPlanSplitter planSplitter = PolicyManager.getInstance()
            .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName()).getIPlanSplitter();

    @Override
    public List<? extends IginxPlan> generateSubPlans(RequestContext requestContext) {
        List<SplitInfo> splitInfoList;
        switch (requestContext.getType()) {
            case CreateDatabase:
                CreateDatabaseReq createDatabaseReq = ((CreateDatabaseContext) requestContext).getReq();
                return SortedListAbstractMetaManager.getInstance().getStorageEngineList().stream().map(StorageEngineMeta::getId)
                        .map(e -> new CreateDatabasePlan(createDatabaseReq.getDatabaseName(), e)).collect(Collectors.toList());
            case DropDatabase:
                DropDatabaseReq dropDatabaseReq = ((DropDatabaseContext) requestContext).getReq();
                return SortedListAbstractMetaManager.getInstance().getStorageEngineList().stream().map(StorageEngineMeta::getId)
                        .map(e -> new DropDatabasePlan(dropDatabaseReq.getDatabaseName(), e)).collect(Collectors.toList());
            case AddColumns:
                AddColumnsReq addColumnsReq = ((AddColumnsContext) requestContext).getReq();
                AddColumnsPlan addColumnsPlan = new AddColumnsPlan(addColumnsReq.getPaths(), addColumnsReq.getAttributesList());
                splitInfoList = planSplitter.getSplitAddColumnsPlanResults(addColumnsPlan);
                return splitAddColumnsPlan(addColumnsPlan, splitInfoList);
            case DeleteColumns:
                DeleteColumnsReq deleteColumnsReq = ((DeleteColumnsContext) requestContext).getReq();
                DeleteColumnsPlan deleteColumnsPlan = new DeleteColumnsPlan(deleteColumnsReq.getPaths());
                splitInfoList = planSplitter.getSplitDeleteColumnsPlanResults(deleteColumnsPlan);
                return splitDeleteColumnsPlan(deleteColumnsPlan, splitInfoList);
            case InsertRecords:
                InsertRecordsReq insertRecordsReq = ((InsertRecordsContext) requestContext).getReq();
                InsertRecordsPlan insertRecordsPlan = new InsertRecordsPlan(
                        insertRecordsReq.getPaths(),
                        getLongArrayFromByteArray(insertRecordsReq.getTimestamps()),
                        getValuesListByDataType(insertRecordsReq.getValuesList(), insertRecordsReq.getDataTypeList()),
                        insertRecordsReq.dataTypeList,
                        insertRecordsReq.getAttributesList()
                );
                splitInfoList = planSplitter.getSplitInsertRecordsPlanResults(insertRecordsPlan);
                return splitInsertRecordsPlan(insertRecordsPlan, splitInfoList);
            case DeleteDataInColumns:
                DeleteDataInColumnsReq deleteDataInColumnsReq = ((DeleteDataInColumnsContext) requestContext).getReq();
                DeleteDataInColumnsPlan deleteDataInColumnsPlan = new DeleteDataInColumnsPlan(
                        deleteDataInColumnsReq.getPaths(),
                        deleteDataInColumnsReq.getStartTime(),
                        deleteDataInColumnsReq.getEndTime()
                );
                splitInfoList = planSplitter.getSplitDeleteDataInColumnsPlanResults(deleteDataInColumnsPlan);
                return splitDeleteDataInColumnsPlan(deleteDataInColumnsPlan, splitInfoList);
            case QueryData:
                QueryDataReq queryDataReq = ((QueryDataContext) requestContext).getReq();
                QueryDataPlan queryDataPlan = new QueryDataPlan(
                        queryDataReq.getPaths(),
                        queryDataReq.getStartTime(),
                        queryDataReq.getEndTime()
                );
                splitInfoList = planSplitter.getSplitQueryDataPlanResults(queryDataPlan);
                return splitQueryDataPlan(queryDataPlan, splitInfoList);
            case AggregateQuery:
                AggregateQueryReq aggregateQueryReq = ((AggregateQueryContext) requestContext).getReq();
                switch (aggregateQueryReq.getAggregateType()) {
                    case MAX:
                        MaxQueryPlan maxQueryPlan = new MaxQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitMaxQueryPlanResults(maxQueryPlan);
                        return splitMaxQueryPlan(maxQueryPlan, splitInfoList);
                    case MIN:
                        MinQueryPlan minQueryPlan = new MinQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitMinQueryPlanResults(minQueryPlan);
                        return splitMinQueryPlan(minQueryPlan, splitInfoList);
                    case FIRST:
                        FirstQueryPlan firstQueryPlan = new FirstQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitFirstQueryPlanResults(firstQueryPlan);
                        return splitFirstQueryPlan(firstQueryPlan, splitInfoList);
                    case LAST:
                        LastQueryPlan lastQueryPlan = new LastQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitLastQueryPlanResults(lastQueryPlan);
                        return splitLastQueryPlan(lastQueryPlan, splitInfoList);
                    case AVG:
                        break;
                    case SUM:
                        SumQueryPlan sumQueryPlan = new SumQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitSumQueryPlanResults(sumQueryPlan);
                        return splitSumQueryPlan(sumQueryPlan, splitInfoList);
                    case COUNT:
                        CountQueryPlan countQueryPlan = new CountQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitCountQueryPlanResults(countQueryPlan);
                        return splitCountQueryPlan(countQueryPlan, splitInfoList);
                    default:
                        throw new UnsupportedOperationException(aggregateQueryReq.getAggregateType().toString());
                }
                break;
            default:
                throw new UnsupportedOperationException(requestContext.getType().toString());
        }
        return null;
    }

    public List<AddColumnsPlan> splitAddColumnsPlan(AddColumnsPlan plan, List<SplitInfo> infoList) {
        List<AddColumnsPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            AddColumnsPlan subPlan = new AddColumnsPlan(plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    plan.getAttributesByInterval(info.getTimeSeriesInterval()));
            subPlan.setSync(info.getReplica().getReplicaIndex() == 0);
            subPlan.setStorageEngineId(info.getReplica().getStorageEngineId());
            plans.add(subPlan);
        }

        return plans;
    }

    public List<DeleteColumnsPlan> splitDeleteColumnsPlan(DeleteColumnsPlan plan, List<SplitInfo> infoList) {
        List<DeleteColumnsPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            DeleteColumnsPlan subPlan = new DeleteColumnsPlan(plan.getPathsByInterval(info.getTimeSeriesInterval()));
            subPlan.setSync(info.getReplica().getReplicaIndex() == 0);
            plans.add(subPlan);
        }

        return plans;
    }

    public List<InsertRecordsPlan> splitInsertRecordsPlan(InsertRecordsPlan plan, List<SplitInfo> infoList) {
        List<InsertRecordsPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            Pair<long[], Pair<Integer, Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByInterval(info.getTimeInterval());
            InsertRecordsPlan subPlan = new InsertRecordsPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    timestampsAndIndexes.k,
                    plan.getValuesByIndexes(timestampsAndIndexes.v, info.getTimeSeriesInterval()),
                    plan.getDataTypeListByInterval(info.getTimeSeriesInterval()),
                    plan.getAttributesByInterval(info.getTimeSeriesInterval()),
                    info.getReplica().getStorageEngineId()
            );
            subPlan.setSync(info.getReplica().getReplicaIndex() == 0);
            plans.add(subPlan);
        }

        return plans;
    }

    public List<DeleteDataInColumnsPlan> splitDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan, List<SplitInfo> infoList) {
        List<DeleteDataInColumnsPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            DeleteDataInColumnsPlan subPlan = new DeleteDataInColumnsPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()),
                    info.getReplica().getStorageEngineId()
            );
            plans.add(subPlan);
        }

        return plans;
    }

    public List<QueryDataPlan> splitQueryDataPlan(QueryDataPlan plan, List<SplitInfo> infoList) {
        List<QueryDataPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            QueryDataPlan subPlan = new QueryDataPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()),
                    info.getReplica().getStorageEngineId()
            );
            plans.add(subPlan);
        }

        return plans;
    }

    public List<MaxQueryPlan> splitMaxQueryPlan(MaxQueryPlan plan, List<SplitInfo> infoList) {
        List<MaxQueryPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            MaxQueryPlan subPlan = new MaxQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()),
                    info.getReplica().getStorageEngineId()
            );
            plans.add(subPlan);
        }

        return plans;
    }

    public List<MinQueryPlan> splitMinQueryPlan(MinQueryPlan plan, List<SplitInfo> infoList) {
        List<MinQueryPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            MinQueryPlan subPlan = new MinQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()),
                    info.getReplica().getStorageEngineId()
            );
            plans.add(subPlan);
        }

        return plans;
    }

    public List<FirstQueryPlan> splitFirstQueryPlan(FirstQueryPlan plan, List<SplitInfo> infoList) {
        List<FirstQueryPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            FirstQueryPlan subPlan = new FirstQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()),
                    info.getReplica().getStorageEngineId()
            );
            plans.add(subPlan);
        }

        return plans;
    }

    public List<LastQueryPlan> splitLastQueryPlan(LastQueryPlan plan, List<SplitInfo> infoList) {
        List<LastQueryPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            LastQueryPlan subPlan = new LastQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()),
                    info.getReplica().getStorageEngineId()
            );
            plans.add(subPlan);
        }

        return plans;
    }

    public List<CountQueryPlan> splitCountQueryPlan(CountQueryPlan plan, List<SplitInfo> infoList) {
        List<CountQueryPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            CountQueryPlan subPlan = new CountQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()),
                    info.getReplica().getStorageEngineId()
            );
            plans.add(subPlan);
        }

        return plans;
    }

    public List<SumQueryPlan> splitSumQueryPlan(SumQueryPlan plan, List<SplitInfo> infoList) {
        List<SumQueryPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            SumQueryPlan subPlan = new SumQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()),
                    info.getReplica().getStorageEngineId()
            );
            plans.add(subPlan);
        }

        return plans;
    }

}
