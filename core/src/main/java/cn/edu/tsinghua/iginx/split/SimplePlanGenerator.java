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
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.DeleteColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DeleteDataInColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.InsertColumnRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertNonAlignedColumnRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertNonAlignedRowRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertRowRecordsContext;
import cn.edu.tsinghua.iginx.core.context.LastQueryContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.core.context.ValueFilterQueryContext;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentStatistics;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.LastValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.ShowColumnsPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.ValueFilterQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.policy.IPlanSplitter;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.InsertColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.LastQueryReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryReq;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
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
        List<SplitInfo> splitInfoList;
        switch (requestContext.getType()) {
            case DeleteColumns:
                DeleteColumnsReq deleteColumnsReq = ((DeleteColumnsContext) requestContext).getReq();
                DeleteColumnsPlan deleteColumnsPlan = new DeleteColumnsPlan(deleteColumnsReq.getPaths());
                splitInfoList = planSplitter.getSplitDeleteColumnsPlanResults(deleteColumnsPlan);
                return splitDeleteColumnsPlan(deleteColumnsPlan, splitInfoList);
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
                if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()
                        && DefaultMetaManager.getInstance().isResharding()
                        && insertColumnRecordsPlan.getEndTime() > DefaultMetaManager.getInstance().getMaxActiveFragmentEndTime()) {
                    Thread thread = Thread.currentThread();
                    DefaultMetaManager.getInstance().addWaitingReshardThread(thread);
                    logger.info("thread {} is parked", thread.getId());
                    LockSupport.park();
                }
                splitInfoList = planSplitter.getSplitInsertColumnRecordsPlanResults(insertColumnRecordsPlan);
                List<InsertColumnRecordsPlan> insertColumnRecordsPlans = splitInsertColumnRecordsPlan(insertColumnRecordsPlan, splitInfoList);
                if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()) {
                    Map<FragmentMeta, FragmentStatistics> statisticsMap = new HashMap<>();
                    for (InsertColumnRecordsPlan plan: insertColumnRecordsPlans) {
                        FragmentStatistics statistics = plan.getStatistics();
                        // TODO 边界条件处理
                        if (statistics != null && plan.getFragment().getTimeInterval().getStartTime() >= DefaultMetaManager.getInstance().getActiveFragmentStartTime()) {
                            statisticsMap.put(plan.getFragment(), statistics);
                        }
                    }
                    DefaultMetaManager.getInstance().updateMaxActiveFragmentEndTime(statisticsMap.values());
                    DefaultMetaManager.getInstance().updateActiveFragmentStatistics(statisticsMap);
                }
                return insertColumnRecordsPlans;
            case InsertNonAlignedColumnRecords:
                InsertNonAlignedColumnRecordsReq insertNonAlignedColumnRecordsReq = ((InsertNonAlignedColumnRecordsContext) requestContext).getReq();
                long[] nonAlignedTimestamps = getLongArrayFromByteArray(insertNonAlignedColumnRecordsReq.getTimestamps());
                InsertNonAlignedColumnRecordsPlan insertNonAlignedColumnRecordsPlan = new InsertNonAlignedColumnRecordsPlan(
                        insertNonAlignedColumnRecordsReq.getPaths(),
                        nonAlignedTimestamps,
                        getColumnValuesByDataType(insertNonAlignedColumnRecordsReq.getValuesList(), insertNonAlignedColumnRecordsReq.getDataTypeList(), insertNonAlignedColumnRecordsReq.getBitmapList(), nonAlignedTimestamps.length),
                        insertNonAlignedColumnRecordsReq.getBitmapList().stream().map(x -> new Bitmap(nonAlignedTimestamps.length, x.array())).collect(Collectors.toList()),
                        insertNonAlignedColumnRecordsReq.getDataTypeList(),
                        insertNonAlignedColumnRecordsReq.getAttributesList()
                );
                if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()
                        && DefaultMetaManager.getInstance().isResharding()
                        && insertNonAlignedColumnRecordsPlan.getEndTime() > DefaultMetaManager.getInstance().getMaxActiveFragmentEndTime()) {
                    Thread thread = Thread.currentThread();
                    DefaultMetaManager.getInstance().addWaitingReshardThread(thread);
                    logger.info("thread {} is parked", thread.getId());
                    LockSupport.park();
                }
                splitInfoList = planSplitter.getSplitInsertNonAlignedColumnRecordsPlanResults(insertNonAlignedColumnRecordsPlan);
                List<InsertNonAlignedColumnRecordsPlan> insertNonAlignedColumnRecordsPlans = splitInsertNonAlignedColumnRecordsPlan(insertNonAlignedColumnRecordsPlan, splitInfoList);
                if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()) {
                    Map<FragmentMeta, FragmentStatistics> statisticsMap = new HashMap<>();
                    for (InsertNonAlignedColumnRecordsPlan plan: insertNonAlignedColumnRecordsPlans) {
                        FragmentStatistics statistics = plan.getStatistics();
                        if (statistics != null && plan.getFragment().getTimeInterval().getStartTime() >= DefaultMetaManager.getInstance().getActiveFragmentStartTime()) {
                            statisticsMap.put(plan.getFragment(), statistics);
                        }
                    }
                    DefaultMetaManager.getInstance().updateMaxActiveFragmentEndTime(statisticsMap.values());
                    DefaultMetaManager.getInstance().updateActiveFragmentStatistics(statisticsMap);
                }
                return insertNonAlignedColumnRecordsPlans;
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
                if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()
                        && DefaultMetaManager.getInstance().isResharding()
                        && insertRowRecordsPlan.getEndTime() > DefaultMetaManager.getInstance().getMaxActiveFragmentEndTime()) {
                    Thread thread = Thread.currentThread();
                    DefaultMetaManager.getInstance().addWaitingReshardThread(thread);
                    logger.info("thread {} is parked", thread.getId());
                    LockSupport.park();
                }
                splitInfoList = planSplitter.getSplitInsertRowRecordsPlanResults(insertRowRecordsPlan);
                List<InsertRowRecordsPlan> insertRowRecordsPlans = splitInsertRowRecordsPlan(insertRowRecordsPlan, splitInfoList);
                if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()) {
                    Map<FragmentMeta, FragmentStatistics> statisticsMap = new HashMap<>();
                    for (InsertRowRecordsPlan plan: insertRowRecordsPlans) {
                        FragmentStatistics statistics = plan.getStatistics();
                        if (statistics != null && plan.getFragment().getTimeInterval().getStartTime() >= DefaultMetaManager.getInstance().getActiveFragmentStartTime()) {
                            statisticsMap.put(plan.getFragment(), statistics);
                        }
                    }
                    DefaultMetaManager.getInstance().updateMaxActiveFragmentEndTime(statisticsMap.values());
                    DefaultMetaManager.getInstance().updateActiveFragmentStatistics(statisticsMap);
                }
                return insertRowRecordsPlans;
            case InsertNonAlignedRowRecords:
                InsertNonAlignedRowRecordsReq insertNonAlignedRowRecordsReq = ((InsertNonAlignedRowRecordsContext) requestContext).getReq();
                InsertNonAlignedRowRecordsPlan insertNonAlignedRowRecordsPlan = new InsertNonAlignedRowRecordsPlan(
                        insertNonAlignedRowRecordsReq.getPaths(),
                        getLongArrayFromByteArray(insertNonAlignedRowRecordsReq.getTimestamps()),
                        getRowValuesByDataType(insertNonAlignedRowRecordsReq.getValuesList(), insertNonAlignedRowRecordsReq.getDataTypeList(), insertNonAlignedRowRecordsReq.getBitmapList()),
                        insertNonAlignedRowRecordsReq.getBitmapList().stream().map(x -> new Bitmap(insertNonAlignedRowRecordsReq.getPathsSize(), x.array())).collect(Collectors.toList()),
                        insertNonAlignedRowRecordsReq.getDataTypeList(),
                        insertNonAlignedRowRecordsReq.getAttributesList()
                );
                if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()
                        && DefaultMetaManager.getInstance().isResharding()
                        && insertNonAlignedRowRecordsPlan.getEndTime() > DefaultMetaManager.getInstance().getMaxActiveFragmentEndTime()) {
                    Thread thread = Thread.currentThread();
                    DefaultMetaManager.getInstance().addWaitingReshardThread(thread);
                    logger.info("thread {} is parked", thread.getId());
                    LockSupport.park();
                }
                splitInfoList = planSplitter.getSplitInsertNonAlignedRowRecordsPlanResults(insertNonAlignedRowRecordsPlan);
                List<InsertNonAlignedRowRecordsPlan> insertNonAlignedRowRecordsPlans = splitInsertNonAlignedRowRecordsPlan(insertNonAlignedRowRecordsPlan, splitInfoList);
                if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()) {
                    Map<FragmentMeta, FragmentStatistics> statisticsMap = new HashMap<>();
                    for (InsertNonAlignedRowRecordsPlan plan: insertNonAlignedRowRecordsPlans) {
                        FragmentStatistics statistics = plan.getStatistics();
                        if (statistics != null && plan.getFragment().getTimeInterval().getStartTime() >= DefaultMetaManager.getInstance().getActiveFragmentStartTime()) {
                            statisticsMap.put(plan.getFragment(), statistics);
                        }
                    }
                    DefaultMetaManager.getInstance().updateMaxActiveFragmentEndTime(statisticsMap.values());
                    DefaultMetaManager.getInstance().updateActiveFragmentStatistics(statisticsMap);
                }
                return insertNonAlignedRowRecordsPlans;
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
                    case FIRST_VALUE:
                        FirstValueQueryPlan firstValueQueryPlan = new FirstValueQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitFirstQueryPlanResults(firstValueQueryPlan);
                        return splitFirstQueryPlan(firstValueQueryPlan, splitInfoList);
                    case LAST_VALUE:
                        LastValueQueryPlan lastValueQueryPlan = new LastValueQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitLastQueryPlanResults(lastValueQueryPlan);
                        return splitLastQueryPlan(lastValueQueryPlan, splitInfoList);
                    case AVG:
                        AvgQueryPlan avgQueryPlan = new AvgQueryPlan(
                                aggregateQueryReq.getPaths(),
                                aggregateQueryReq.getStartTime(),
                                aggregateQueryReq.getEndTime()
                        );
                        splitInfoList = planSplitter.getSplitAvgQueryPlanResults(avgQueryPlan);
                        return splitAvgQueryPlan(avgQueryPlan, splitInfoList);
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
                        splitInfoList = planSplitter.getSplitDownsampleMaxQueryPlanResults(downsampleMaxQueryPlan);
                        return splitDownsampleMaxQueryPlan(downsampleMaxQueryPlan, splitInfoList);
                    case MIN:
                        DownsampleMinQueryPlan downsampleMinQueryPlan = new DownsampleMinQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        splitInfoList = planSplitter.getSplitDownsampleMinQueryPlanResults(downsampleMinQueryPlan);
                        return splitDownsampleMinQueryPlan(downsampleMinQueryPlan, splitInfoList);
                    case AVG:
                        DownsampleAvgQueryPlan downsampleAvgQueryPlan = new DownsampleAvgQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        splitInfoList = planSplitter.getSplitDownsampleAvgQueryPlanResults(downsampleAvgQueryPlan);
                        return splitDownsampleAvgQueryPlan(downsampleAvgQueryPlan, splitInfoList);
                    case SUM:
                        DownsampleSumQueryPlan downsampleSumQueryPlan = new DownsampleSumQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        splitInfoList = planSplitter.getSplitDownsampleSumQueryPlanResults(downsampleSumQueryPlan);
                        return splitDownsampleSumQueryPlan(downsampleSumQueryPlan, splitInfoList);
                    case COUNT:
                        DownsampleCountQueryPlan downsampleCountQueryPlan = new DownsampleCountQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        splitInfoList = planSplitter.getSplitDownsampleCountQueryPlanResults(downsampleCountQueryPlan);
                        return splitDownsampleCountQueryPlan(downsampleCountQueryPlan, splitInfoList);
                    case FIRST_VALUE:
                        DownsampleFirstValueQueryPlan downsampleFirstValueQueryPlan = new DownsampleFirstValueQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        splitInfoList = planSplitter.getSplitDownsampleFirstQueryPlanResults(downsampleFirstValueQueryPlan);
                        return splitDownsampleFirstQueryPlan(downsampleFirstValueQueryPlan, splitInfoList);
                    case LAST_VALUE:
                        DownsampleLastValueQueryPlan downsampleLastValueQueryPlan = new DownsampleLastValueQueryPlan(
                                downsampleQueryReq.getPaths(),
                                downsampleQueryReq.getStartTime(),
                                downsampleQueryReq.getEndTime(),
                                downsampleQueryReq.getPrecision()
                        );
                        splitInfoList = planSplitter.getSplitDownsampleLastQueryPlanResults(downsampleLastValueQueryPlan);
                        return splitDownsampleLastQueryPlan(downsampleLastValueQueryPlan, splitInfoList);
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
                splitInfoList = planSplitter.getValueFilterQueryPlanResults(valueFilterQueryPlan);
                return splitValueFilterQueryPlan(valueFilterQueryPlan, splitInfoList);
            }
            case LastQuery:
                LastQueryReq lastQueryReq = ((LastQueryContext) requestContext).getReq();
                LastQueryPlan lastQueryPlan = new LastQueryPlan(
                        lastQueryReq.getPaths(),
                        lastQueryReq.getStartTime()
                );
                splitInfoList = planSplitter.getLastQueryPlanResults(lastQueryPlan);
                return splitLastQueryPlan(lastQueryPlan, splitInfoList);
            case ShowColumns:
                return planSplitter.getSplitShowColumnsPlanResult().stream().map(ShowColumnsPlan::new).collect(Collectors.toList());
            default:
                throw new UnsupportedOperationException(requestContext.getType().toString());
        }
    }

    public List<DeleteColumnsPlan> splitDeleteColumnsPlan(DeleteColumnsPlan plan, List<SplitInfo> infoList) {
        List<DeleteColumnsPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            DeleteColumnsPlan subPlan = new DeleteColumnsPlan(
                    paths,
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<InsertColumnRecordsPlan> splitInsertColumnRecordsPlan(InsertColumnRecordsPlan plan, List<SplitInfo> infoList) {
        List<InsertColumnRecordsPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            Pair<long[], Pair<Integer, Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByInterval(info.getTimeInterval());
            Pair<Object[], List<Bitmap>> valuesAndBitmaps = plan.getValuesAndBitmapsByIndexes(timestampsAndIndexes.v, info.getTimeSeriesInterval());
            if (valuesAndBitmaps.k.length == 0) {
                continue;
            }
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            InsertColumnRecordsPlan subPlan = new InsertColumnRecordsPlan(
                    paths,
                    timestampsAndIndexes.k,
                    valuesAndBitmaps.k,
                    valuesAndBitmaps.v,
                    plan.getDataTypeListByInterval(info.getTimeSeriesInterval()),
                    plan.getAttributesByInterval(info.getTimeSeriesInterval()),
                    info.getStorageUnit(),
                    info.getFragment()
            );
            subPlan.setSync(info.getStorageUnit().isMaster());
            plans.add(subPlan);
        }
        return plans;
    }

    public List<InsertNonAlignedColumnRecordsPlan> splitInsertNonAlignedColumnRecordsPlan(InsertNonAlignedColumnRecordsPlan plan, List<SplitInfo> infoList) {
        List<InsertNonAlignedColumnRecordsPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            Pair<long[], Pair<Integer, Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByInterval(info.getTimeInterval());
            Pair<Object[], List<Bitmap>> valuesAndBitmaps = plan.getValuesAndBitmapsByIndexes(timestampsAndIndexes.v, info.getTimeSeriesInterval());
            if (valuesAndBitmaps.k.length == 0) {
                continue;
            }
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            InsertNonAlignedColumnRecordsPlan subPlan = new InsertNonAlignedColumnRecordsPlan(
                    paths,
                    timestampsAndIndexes.k,
                    valuesAndBitmaps.k,
                    valuesAndBitmaps.v,
                    plan.getDataTypeListByInterval(info.getTimeSeriesInterval()),
                    plan.getAttributesByInterval(info.getTimeSeriesInterval()),
                    info.getStorageUnit(),
                    info.getFragment()
            );
            subPlan.setSync(info.getStorageUnit().isMaster());
            plans.add(subPlan);
        }
        return plans;
    }

    public List<InsertRowRecordsPlan> splitInsertRowRecordsPlan(InsertRowRecordsPlan plan, List<SplitInfo> infoList) {
        List<InsertRowRecordsPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            Pair<long[], Pair<Integer, Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByInterval(info.getTimeInterval());
            Pair<Object[], List<Bitmap>> valuesAndBitmaps = plan.getValuesAndBitmapsByIndexes(timestampsAndIndexes.v, info.getTimeSeriesInterval());
            if (valuesAndBitmaps.k.length == 0) {
                continue;
            }
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            InsertRowRecordsPlan subPlan = new InsertRowRecordsPlan(
                    paths,
                    timestampsAndIndexes.k,
                    valuesAndBitmaps.k,
                    valuesAndBitmaps.v,
                    plan.getDataTypeListByInterval(info.getTimeSeriesInterval()),
                    plan.getAttributesByInterval(info.getTimeSeriesInterval()),
                    info.getStorageUnit(),
                    info.getFragment()
            );
            subPlan.setSync(info.getStorageUnit().isMaster());
            plans.add(subPlan);
        }
        return plans;
    }

    public List<InsertNonAlignedRowRecordsPlan> splitInsertNonAlignedRowRecordsPlan(InsertNonAlignedRowRecordsPlan plan, List<SplitInfo> infoList) {
        List<InsertNonAlignedRowRecordsPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            Pair<long[], Pair<Integer, Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByInterval(info.getTimeInterval());
            Pair<Object[], List<Bitmap>> valuesAndBitmaps = plan.getValuesAndBitmapsByIndexes(timestampsAndIndexes.v, info.getTimeSeriesInterval());
            if (valuesAndBitmaps.k.length == 0) {
                continue;
            }
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            InsertNonAlignedRowRecordsPlan subPlan = new InsertNonAlignedRowRecordsPlan(
                    paths,
                    timestampsAndIndexes.k,
                    valuesAndBitmaps.k,
                    valuesAndBitmaps.v,
                    plan.getDataTypeListByInterval(info.getTimeSeriesInterval()),
                    plan.getAttributesByInterval(info.getTimeSeriesInterval()),
                    info.getStorageUnit(),
                    info.getFragment()
            );
            subPlan.setSync(info.getStorageUnit().isMaster());
            plans.add(subPlan);
        }
        return plans;
    }

    public List<DeleteDataInColumnsPlan> splitDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan, List<SplitInfo> infoList) {
        List<DeleteDataInColumnsPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            DeleteDataInColumnsPlan subPlan = new DeleteDataInColumnsPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<QueryDataPlan> splitQueryDataPlan(QueryDataPlan plan, List<SplitInfo> infoList) {
        List<QueryDataPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            QueryDataPlan subPlan = new QueryDataPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<ValueFilterQueryPlan> splitValueFilterQueryPlan(ValueFilterQueryPlan plan, List<SplitInfo> infoList) {
        List<ValueFilterQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            ValueFilterQueryPlan subPlan = new ValueFilterQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    plan.getBooleanExpression(),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<LastQueryPlan> splitLastQueryPlan(LastQueryPlan plan, List<SplitInfo> infoList) {
        List<LastQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            LastQueryPlan subPlan = new LastQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<MaxQueryPlan> splitMaxQueryPlan(MaxQueryPlan plan, List<SplitInfo> infoList) {
        List<MaxQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            MaxQueryPlan subPlan = new MaxQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    private List<IginxPlan> splitDownsampleQueryPlan(DownsampleQueryPlan plan, List<SplitInfo> infoList) {
        List<IginxPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            IginxPlan subPlan = null;
            switch (info.getType()) {
                case MAX:
                    subPlan = new MaxQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case MIN:
                    subPlan = new MinQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case FIRST_VALUE:
                    subPlan = new FirstValueQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case LAST_VALUE:
                    subPlan = new LastValueQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case AVG:
                    subPlan = new AvgQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case SUM:
                    subPlan = new SumQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case COUNT:
                    subPlan = new CountQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_MAX:
                    subPlan = new DownsampleMaxQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_MIN:
                    subPlan = new DownsampleMinQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_FIRST:
                    subPlan = new DownsampleFirstValueQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_LAST:
                    subPlan = new DownsampleLastValueQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_AVG:
                    subPlan = new DownsampleAvgQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_SUM:
                    subPlan = new DownsampleSumQueryPlan(
                            paths,
                            info.getTimeInterval().getStartTime(),
                            info.getTimeInterval().getEndTime(),
                            plan.getPrecision(),
                            info.getStorageUnit()
                    );
                    break;
                case DOWNSAMPLE_COUNT:
                    subPlan = new DownsampleCountQueryPlan(
                            paths,
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

    public List<IginxPlan> splitDownsampleMaxQueryPlan(DownsampleMaxQueryPlan plan, List<SplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<MinQueryPlan> splitMinQueryPlan(MinQueryPlan plan, List<SplitInfo> infoList) {
        List<MinQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            MinQueryPlan subPlan = new MinQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleMinQueryPlan(DownsampleMinQueryPlan plan, List<SplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<FirstValueQueryPlan> splitFirstQueryPlan(FirstValueQueryPlan plan, List<SplitInfo> infoList) {
        List<FirstValueQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            FirstValueQueryPlan subPlan = new FirstValueQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleFirstQueryPlan(DownsampleFirstValueQueryPlan plan, List<SplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<LastValueQueryPlan> splitLastQueryPlan(LastValueQueryPlan plan, List<SplitInfo> infoList) {
        List<LastValueQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            LastValueQueryPlan subPlan = new LastValueQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleLastQueryPlan(DownsampleLastValueQueryPlan plan, List<SplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<CountQueryPlan> splitCountQueryPlan(CountQueryPlan plan, List<SplitInfo> infoList) {
        List<CountQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            CountQueryPlan subPlan = new CountQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleCountQueryPlan(DownsampleCountQueryPlan plan, List<SplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<SumQueryPlan> splitSumQueryPlan(SumQueryPlan plan, List<SplitInfo> infoList) {
        List<SumQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            SumQueryPlan subPlan = new SumQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleSumQueryPlan(DownsampleSumQueryPlan plan, List<SplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

    public List<AvgQueryPlan> splitAvgQueryPlan(AvgQueryPlan plan, List<SplitInfo> infoList) {
        List<AvgQueryPlan> plans = new ArrayList<>();
        for (SplitInfo info : infoList) {
            List<String> paths = plan.getPathsByInterval(info.getTimeSeriesInterval());
            if (paths.size() == 0) {
                continue;
            }
            AvgQueryPlan subPlan = new AvgQueryPlan(
                    paths,
                    Math.max(plan.getStartTime(), info.getTimeInterval().getStartTime()),
                    Math.min(plan.getEndTime(), info.getTimeInterval().getEndTime()),
                    info.getStorageUnit()
            );
            plans.add(subPlan);
        }
        return plans;
    }

    public List<IginxPlan> splitDownsampleAvgQueryPlan(DownsampleAvgQueryPlan plan, List<SplitInfo> infoList) {
        return splitDownsampleQueryPlan(plan, infoList);
    }

}
