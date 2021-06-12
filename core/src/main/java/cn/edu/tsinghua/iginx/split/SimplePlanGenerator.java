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
import cn.edu.tsinghua.iginx.core.context.DeleteColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DeleteDataInColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.InsertColumnRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertRowRecordsContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.core.context.ValueFilterQueryContext;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.ValueFilterQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.policy.IPlanSplitter;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.thrift.AddColumnsReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.InsertColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryReq;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getColumnValuesByDataType;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteArray;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getRowValuesByDataType;

public class SimplePlanGenerator implements IPlanGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SimplePlanGenerator.class);

    private final IPlanSplitter planSplitter = PolicyManager.getInstance()
            .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName()).getIPlanSplitter();

    @Override
    public List<? extends IginxPlan> generateSubPlans(RequestContext requestContext) {
        switch (requestContext.getType()) {
            case CreateDatabase:
            case DropDatabase:
                return null;
            case AddColumns:
                AddColumnsReq addColumnsReq = ((AddColumnsContext) requestContext).getReq();
                AddColumnsPlan addColumnsPlan = new AddColumnsPlan(addColumnsReq.getPaths(), addColumnsReq.getAttributesList());
                List<NonInsertSplitInfo> addColumnsSplitInfoList = planSplitter.getSplitAddColumnsPlanResults(addColumnsPlan);
                return splitAddColumnsPlan(addColumnsPlan, addColumnsSplitInfoList);
            case DeleteColumns:
                DeleteColumnsReq deleteColumnsReq = ((DeleteColumnsContext) requestContext).getReq();
                DeleteColumnsPlan deleteColumnsPlan = new DeleteColumnsPlan(deleteColumnsReq.getPaths());
                List<NonInsertSplitInfo> deleteColumnsSplitInfoList = planSplitter.getSplitDeleteColumnsPlanResults(deleteColumnsPlan);
                return splitDeleteColumnsPlan(deleteColumnsPlan, deleteColumnsSplitInfoList);
            case InsertColumnRecords:
                InsertColumnRecordsReq insertColumnRecordsReq = ((InsertColumnRecordsContext) requestContext).getReq();
                long[] timestamps = getLongArrayFromByteArray(insertColumnRecordsReq.getTimestamps());
                InsertColumnRecordsPlan insertColumnRecordsPlan = new InsertColumnRecordsPlan(
                        insertColumnRecordsReq.getPaths(),
                        timestamps,
                        getColumnValuesByDataType(insertColumnRecordsReq.getValuesList(), insertColumnRecordsReq.getDataTypeList(), insertColumnRecordsReq.getBitmapList(), timestamps.length),
                        insertColumnRecordsReq.getBitmapList().stream().map(x -> new Bitmap(timestamps.length, x.array())).collect(Collectors.toList()),
                        insertColumnRecordsReq.getDataTypeList(),
                        insertColumnRecordsReq.getAttributesList()
                );
                List<InsertSplitInfo> insertColumnRecordsSplitInfoList = planSplitter.getSplitInsertColumnRecordsPlanResults(insertColumnRecordsPlan);
                return splitInsertColumnRecordsPlan(insertColumnRecordsPlan, insertColumnRecordsSplitInfoList);
            case InsertRowRecords:
                InsertRowRecordsReq insertRowRecordsReq = ((InsertRowRecordsContext) requestContext).getReq();
                InsertRowRecordsPlan insertRowRecordsPlan = new InsertRowRecordsPlan(
                        insertRowRecordsReq.getPaths(),
                        getLongArrayFromByteArray(insertRowRecordsReq.getTimestamps()),
                        getRowValuesByDataType(insertRowRecordsReq.getValuesList(), insertRowRecordsReq.getDataTypeList(), insertRowRecordsReq.getBitmapList()),
                        insertRowRecordsReq.getBitmapList().stream().map(x -> new Bitmap(insertRowRecordsReq.getPathsSize(), x.array())).collect(Collectors.toList()),
                        insertRowRecordsReq.getDataTypeList(),
                        insertRowRecordsReq.getAttributesList()
                );
                List<InsertSplitInfo> insertRowRecordsSplitInfoList = planSplitter.getSplitInsertRowRecordsPlanResults(insertRowRecordsPlan);
                return splitInsertRowRecordsPlan(insertRowRecordsPlan, insertRowRecordsSplitInfoList);
            case DeleteDataInColumns:
                DeleteDataInColumnsReq deleteDataInColumnsReq = ((DeleteDataInColumnsContext) requestContext).getReq();
                DeleteDataInColumnsPlan deleteDataInColumnsPlan = new DeleteDataInColumnsPlan(
                        deleteDataInColumnsReq.getPaths(),
                        deleteDataInColumnsReq.getStartTime(),
                        deleteDataInColumnsReq.getEndTime()
                );
                List<NonInsertSplitInfo> deleteDataInColumnsSplitInfoList = planSplitter.getSplitDeleteDataInColumnsPlanResults(deleteDataInColumnsPlan);
                return splitDeleteDataInColumnsPlan(deleteDataInColumnsPlan, deleteDataInColumnsSplitInfoList);
            case QueryData:
                QueryDataReq queryDataReq = ((QueryDataContext) requestContext).getReq();
                QueryDataPlan queryDataPlan = new QueryDataPlan(
                        queryDataReq.getPaths(),
                        queryDataReq.getStartTime(),
                        queryDataReq.getEndTime()
                );
                List<NonInsertSplitInfo> queryDataSplitInfoList = planSplitter.getSplitQueryDataPlanResults(queryDataPlan);
                return splitQueryDataPlan(queryDataPlan, queryDataSplitInfoList);
            case AggregateQuery:
                AggregateQueryReq aggregateQueryReq = ((AggregateQueryContext) requestContext).getReq();
                switch (aggregateQueryReq.getAggregateType()) {
                    case MAX:
                        MaxQueryPlan maxQueryPlan = new MaxQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        List<NonInsertSplitInfo> maxQuerySplitInfoList = planSplitter.getSplitMaxQueryPlanResults(maxQueryPlan);
                        return splitMaxQueryPlan(maxQueryPlan, maxQuerySplitInfoList);
                    case MIN:
                        MinQueryPlan minQueryPlan = new MinQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        List<NonInsertSplitInfo> minQuerySplitInfoList = planSplitter.getSplitMinQueryPlanResults(minQueryPlan);
                        return splitMinQueryPlan(minQueryPlan, minQuerySplitInfoList);
                    case FIRST:
                        FirstQueryPlan firstQueryPlan = new FirstQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        List<NonInsertSplitInfo> firstQuerySplitInfoList = planSplitter.getSplitFirstQueryPlanResults(firstQueryPlan);
                        return splitFirstQueryPlan(firstQueryPlan, firstQuerySplitInfoList);
                    case LAST:
                        LastQueryPlan lastQueryPlan = new LastQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        List<NonInsertSplitInfo> lastQuerySplitInfoList = planSplitter.getSplitLastQueryPlanResults(lastQueryPlan);
                        return splitLastQueryPlan(lastQueryPlan, lastQuerySplitInfoList);
                    case AVG:
                        AvgQueryPlan avgQueryPlan = new AvgQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        List<NonInsertSplitInfo> avgQuerySplitInfoList = planSplitter.getSplitAvgQueryPlanResults(avgQueryPlan);
                        return splitAvgQueryPlan(avgQueryPlan, avgQuerySplitInfoList);
                    case SUM:
                        SumQueryPlan sumQueryPlan = new SumQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        List<NonInsertSplitInfo> sumQuerySplitInfoList = planSplitter.getSplitSumQueryPlanResults(sumQueryPlan);
                        return splitSumQueryPlan(sumQueryPlan, sumQuerySplitInfoList);
                    case COUNT:
                        CountQueryPlan countQueryPlan = new CountQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        List<NonInsertSplitInfo> countQuerySplitInfoList = planSplitter.getSplitCountQueryPlanResults(countQueryPlan);
                        return splitCountQueryPlan(countQueryPlan, countQuerySplitInfoList);
                    default:
                        throw new UnsupportedOperationException(aggregateQueryReq.getAggregateType().toString());
                }
            case DownsampleQuery:
                DownsampleQueryReq downsampleQueryReq = ((DownsampleQueryContext) requestContext).getReq();
                switch (downsampleQueryReq.aggregateType) {
                    case MAX:
                        DownsampleMaxQueryPlan downsampleMaxQueryPlan = new DownsampleMaxQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        List<NonInsertSplitInfo> downsampleMaxQuerySplitInfoList = planSplitter.getSplitDownsampleMaxQueryPlanResults(downsampleMaxQueryPlan);
                        return splitDownsampleMaxQueryPlan(downsampleMaxQueryPlan, downsampleMaxQuerySplitInfoList);
                    case MIN:
                        DownsampleMinQueryPlan downsampleMinQueryPlan = new DownsampleMinQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        List<NonInsertSplitInfo> downsampleMinQuerySplitInfoList = planSplitter.getSplitDownsampleMinQueryPlanResults(downsampleMinQueryPlan);
                        return splitDownsampleMinQueryPlan(downsampleMinQueryPlan, downsampleMinQuerySplitInfoList);
                    case AVG:
                        DownsampleAvgQueryPlan downsampleAvgQueryPlan = new DownsampleAvgQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        List<NonInsertSplitInfo> downsampleAvgQuerySplitInfoList = planSplitter.getSplitDownsampleAvgQueryPlanResults(downsampleAvgQueryPlan);
                        return splitDownsampleAvgQueryPlan(downsampleAvgQueryPlan, downsampleAvgQuerySplitInfoList);
                    case SUM:
                        DownsampleSumQueryPlan downsampleSumQueryPlan = new DownsampleSumQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        List<NonInsertSplitInfo> downsampleSumQuerySplitInfoList = planSplitter.getSplitDownsampleSumQueryPlanResults(downsampleSumQueryPlan);
                        return splitDownsampleSumQueryPlan(downsampleSumQueryPlan, downsampleSumQuerySplitInfoList);
                    case COUNT:
                        DownsampleCountQueryPlan downsampleCountQueryPlan = new DownsampleCountQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        List<NonInsertSplitInfo> downsampleCountQuerySplitInfoList = planSplitter.getSplitDownsampleCountQueryPlanResults(downsampleCountQueryPlan);
                        return splitDownsampleCountQueryPlan(downsampleCountQueryPlan, downsampleCountQuerySplitInfoList);
                    case FIRST:
                        DownsampleFirstQueryPlan downsampleFirstQueryPlan = new DownsampleFirstQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        List<NonInsertSplitInfo> downsampleFirstQuerySplitInfoList = planSplitter.getSplitDownsampleFirstQueryPlanResults(downsampleFirstQueryPlan);
                        return splitDownsampleFirstQueryPlan(downsampleFirstQueryPlan, downsampleFirstQuerySplitInfoList);
                    case LAST:
                        DownsampleLastQueryPlan downsampleLastQueryPlan = new DownsampleLastQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        List<NonInsertSplitInfo> downsampleLastQuerySplitInfoList = planSplitter.getSplitDownsampleLastQueryPlanResults(downsampleLastQueryPlan);
                        return splitDownsampleLastQueryPlan(downsampleLastQueryPlan, downsampleLastQuerySplitInfoList);
                    default:
                        throw new UnsupportedOperationException(downsampleQueryReq.getAggregateType().toString());
                }
            case ValueFilterQuery: {
                ValueFilterQueryReq valueFilterQueryReq = ((ValueFilterQueryContext) requestContext).getReq();
                ValueFilterQueryPlan valueFilterQueryPlan = new ValueFilterQueryPlan(
                        valueFilterQueryReq.getPaths(),
                        valueFilterQueryReq.getStartTime(),
                        valueFilterQueryReq.getEndTime(),
                        ((ValueFilterQueryContext) requestContext).getBooleanExpression()
                );
                List<NonInsertSplitInfo> valueFilterQuerySplitInfoList = planSplitter.getValueFilterQueryPlanResults(valueFilterQueryPlan);
                return splitValueFilterQueryPlan(valueFilterQueryPlan, valueFilterQuerySplitInfoList);
            }
            default:
                throw new UnsupportedOperationException(requestContext.getType().toString());
        }
    }

    public List<AddColumnsPlan> splitAddColumnsPlan(AddColumnsPlan plan, List<NonInsertSplitInfo> infoList) {
        List<AddColumnsPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            AddColumnsPlan subPlan = new AddColumnsPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    plan.getAttributesByInterval(info.getTimeSeriesInterval()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<DeleteColumnsPlan> splitDeleteColumnsPlan(DeleteColumnsPlan plan, List<NonInsertSplitInfo> infoList) {
        List<DeleteColumnsPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            DeleteColumnsPlan subPlan = new DeleteColumnsPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<InsertColumnRecordsPlan> splitInsertColumnRecordsPlan(InsertColumnRecordsPlan plan, List<InsertSplitInfo> infoList) {
        List<InsertColumnRecordsPlan> plans = new ArrayList<>();
        for (InsertSplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getIdealTsInterval());
            Pair<long[], Pair<Integer, Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByInterval(info.getIdealTimeInterval());
            Pair<Object[], List<Bitmap>> valuesAndBitmaps = plan.getValuesAndBitmapsByIndexes(timestampsAndIndexes.v, info.getIdealTsInterval());
            InsertColumnRecordsPlan subPlan = new InsertColumnRecordsPlan(
                    paths,
                    timestampsAndIndexes.k,
                    valuesAndBitmaps.k,
                    valuesAndBitmaps.v,
                    plan.getDataTypeListByInterval(info.getIdealTsInterval()),
                    plan.getAttributesByInterval(info.getIdealTsInterval()),
                    info.getStorageUnit()
            );
            subPlan.setSync(info.getStorageUnit().isMaster());
            plans.add(subPlan);

            // 更新分片的实际边界
            boolean needToUpdate = false;
            String actualStartPath;
            String actualEndPath;
            long actualStartTime;
            long actualEndTime;
            if (info.getActualTsInterval().getStartTimeSeries() == null || paths.get(0).compareTo(info.getActualTsInterval().getStartTimeSeries()) < 0) {
                needToUpdate = true;
                actualStartPath = paths.get(0);
            } else {
                actualStartPath = info.getActualTsInterval().getStartTimeSeries();
            }
            if (info.getActualTsInterval().getEndTimeSeries() == null || paths.get(paths.size() - 1).compareTo(info.getActualTsInterval().getEndTimeSeries()) > 0) {
                needToUpdate = true;
                actualEndPath = paths.get(paths.size() - 1);
            } else {
                actualEndPath = info.getActualTsInterval().getEndTimeSeries();
            }
            if (timestampsAndIndexes.k[0] < info.getActualTimeInterval().getStartTime()) {
                needToUpdate = true;
                actualStartTime = timestampsAndIndexes.k[0];
            } else {
                actualStartTime = info.getActualTimeInterval().getStartTime();
            }
            if (timestampsAndIndexes.k[timestampsAndIndexes.k.length - 1] > info.getActualTimeInterval().getEndTime()) {
                needToUpdate = true;
                actualEndTime = timestampsAndIndexes.k[timestampsAndIndexes.k.length - 1];
            } else {
                actualEndTime = info.getActualTimeInterval().getEndTime();
            }
            if (needToUpdate) {
                DefaultMetaCache.getInstance().updateFragment(
                        new FragmentMeta(
                                info.getIdealTsInterval(),
                                info.getIdealTimeInterval(),
                                new TimeSeriesInterval(actualStartPath, actualEndPath),
                                new TimeInterval(actualStartTime, actualEndTime),
                                info.getStorageUnit()
                        )
                );
            }
        }
        return plans;
    }

    public List<InsertRowRecordsPlan> splitInsertRowRecordsPlan(InsertRowRecordsPlan plan, List<InsertSplitInfo> infoList) {
        List<InsertRowRecordsPlan> plans = new ArrayList<>();
        for (InsertSplitInfo info : infoList) {
            Pair<long[], Pair<Integer, Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByInterval(info.getIdealTimeInterval());
            Pair<Object[], List<Bitmap>> valuesAndBitmaps = plan.getValuesAndBitmapsByIndexes(timestampsAndIndexes.v, info.getIdealTsInterval());
            InsertRowRecordsPlan subPlan = new InsertRowRecordsPlan(
                    plan.getPathsByInterval(info.getIdealTsInterval()),
                    timestampsAndIndexes.k,
                    valuesAndBitmaps.k,
                    valuesAndBitmaps.v,
                    plan.getDataTypeListByInterval(info.getIdealTsInterval()),
                    plan.getAttributesByInterval(info.getIdealTsInterval()),
                    info.getStorageUnit()
            );
            subPlan.setSync(info.getStorageUnit().isMaster());
            plans.add(subPlan);
        }
        return plans;
    }

    public List<DeleteDataInColumnsPlan> splitDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan, List<NonInsertSplitInfo> infoList) {
        List<DeleteDataInColumnsPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            DeleteDataInColumnsPlan subPlan = new DeleteDataInColumnsPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<QueryDataPlan> splitQueryDataPlan(QueryDataPlan plan, List<NonInsertSplitInfo> infoList) {
        List<QueryDataPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            QueryDataPlan subPlan = new QueryDataPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<ValueFilterQueryPlan> splitValueFilterQueryPlan(ValueFilterQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<ValueFilterQueryPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            ValueFilterQueryPlan subPlan = new ValueFilterQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    plan.getBooleanExpression(),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<MaxQueryPlan> splitMaxQueryPlan(MaxQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<MaxQueryPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            MaxQueryPlan subPlan = new MaxQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    private List<IginxPlan> splitDownsampleQueryPlan(DownsampleQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<IginxPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            IginxPlan subPlan = null;
            switch (info.getType()) {
                case MAX:
                    subPlan = new MaxQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case MIN:
                    subPlan = new MinQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case FIRST:
                    subPlan = new FirstQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case LAST:
                    subPlan = new LastQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case AVG:
                    subPlan = new AvgQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case SUM:
                    subPlan = new SumQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case COUNT:
                    subPlan = new CountQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_MAX:
                    subPlan = new DownsampleMaxQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_MIN:
                    subPlan = new DownsampleMinQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_FIRST:
                    subPlan = new DownsampleFirstQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_LAST:
                    subPlan = new DownsampleLastQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_AVG:
                    subPlan = new DownsampleAvgQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_SUM:
                    subPlan = new DownsampleSumQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_COUNT:
                    subPlan = new DownsampleCountQueryPlan(
                            plan.getPathsByInterval(info.getTimeSeriesInterval()),
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
            }
            if (subPlan != null) {
                subPlan.setCombineGroup(info.getCombineGroup());
            }
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleMaxQueryPlan(DownsampleMaxQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<MinQueryPlan> splitMinQueryPlan(MinQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<MinQueryPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            MinQueryPlan subPlan = new MinQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleMinQueryPlan(DownsampleMinQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<FirstQueryPlan> splitFirstQueryPlan(FirstQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<FirstQueryPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            FirstQueryPlan subPlan = new FirstQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleFirstQueryPlan(DownsampleFirstQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<LastQueryPlan> splitLastQueryPlan(LastQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<LastQueryPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            LastQueryPlan subPlan = new LastQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleLastQueryPlan(DownsampleLastQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<CountQueryPlan> splitCountQueryPlan(CountQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<CountQueryPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            CountQueryPlan subPlan = new CountQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleCountQueryPlan(DownsampleCountQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<SumQueryPlan> splitSumQueryPlan(SumQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<SumQueryPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            SumQueryPlan subPlan = new SumQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleSumQueryPlan(DownsampleSumQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<AvgQueryPlan> splitAvgQueryPlan(AvgQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        List<AvgQueryPlan> plans = new ArrayList<>();
        for (NonInsertSplitInfo info : infoList) {
            AvgQueryPlan subPlan = new AvgQueryPlan(
                    plan.getPathsByInterval(info.getTimeSeriesInterval()),
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleAvgQueryPlan(DownsampleAvgQueryPlan plan, List<NonInsertSplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

}
