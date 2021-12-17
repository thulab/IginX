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
package cn.edu.tsinghua.iginx.combine.downsample;

import cn.edu.tsinghua.iginx.combine.aggregate.AggregateCombiner;
import cn.edu.tsinghua.iginx.combine.querydata.QueryExecuteDataSetWrapper;
import cn.edu.tsinghua.iginx.combine.utils.GroupByUtils;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.plan.AggregateQueryPlan;
import cn.edu.tsinghua.iginx.plan.DataPlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DownsampleQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.CheckedFunction;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DownsampleCombiner {

    private static final Logger logger = LoggerFactory.getLogger(DownsampleCombiner.class);

    static Table combineDownsampleQueryTable(List<PlanExecuteResult> planExecuteResults, AggregateType aggregateType, List<Integer> groupByLevels)
            throws ExecutionException {
        Map<Integer, List<PlanExecuteResult>> aggregateResultGroups = planExecuteResults.stream().filter(e -> e.getPlan().getIginxPlanType().isAggregateQuery()).collect(Collectors
                .groupingBy(e -> e.getPlan().getCombineGroup()));
        List<QueryExecuteDataSet> downsampleQueryPlanExecuteResults = planExecuteResults.stream().filter(e -> e.getPlan().getIginxPlanType().isDownsampleQuery())
                .map(DownsampleQueryPlanExecuteResult.class::cast).map(DownsampleQueryPlanExecuteResult::getQueryExecuteDataSets).flatMap(List::stream).collect(Collectors.toList());
        for (List<PlanExecuteResult> aggregateResultGroup : aggregateResultGroups.values()) {
            long timestamp = aggregateResultGroup.stream().map(PlanExecuteResult::getPlan).map(AggregateQueryPlan.class::cast).mapToLong(DataPlan::getStartTime)
                    .min().orElse(0);
            AggregateQueryResp aggregateQueryResp = new AggregateQueryResp();
            switch (aggregateType) {
                case MAX:
                    AggregateCombiner.getInstance().combineMaxResult(aggregateQueryResp, aggregateResultGroup.stream()
                            .map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                    break;
                case MIN:
                    AggregateCombiner.getInstance().combineMinResult(aggregateQueryResp, aggregateResultGroup.stream()
                            .map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                    break;
                case AVG:
                    AggregateCombiner.getInstance().combineAvgResult(aggregateQueryResp, aggregateResultGroup.stream()
                            .map(AvgAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()), false);
                    break;
                case COUNT:
                case SUM:
                    AggregateCombiner.getInstance().combineSumOrCountResult(aggregateQueryResp, aggregateResultGroup.stream()
                            .map(StatisticsAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                    break;
                case FIRST_VALUE:
                    AggregateCombiner.getInstance().combineFirstResult(aggregateQueryResp, aggregateResultGroup.stream()
                            .map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                    break;
                case LAST_VALUE:
                    AggregateCombiner.getInstance().combineLastResult(aggregateQueryResp, aggregateResultGroup.stream()
                            .map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                    break;
            }
            downsampleQueryPlanExecuteResults.add(new DownsampleGroupQueryExecuteDataSet(timestamp, aggregateQueryResp));
        }
        Table table = combineResult(downsampleQueryPlanExecuteResults);
        if (groupByLevels != null && !groupByLevels.isEmpty()) {
            table = groupTable(table, groupByLevels, aggregateType);
        }
        return table;
    }

    public static void combineDownsampleQueryResult(DownsampleQueryResp resp, List<PlanExecuteResult> planExecuteResults, AggregateType aggregateType, List<Integer> groupByLevels)
            throws ExecutionException {
        constructDownsampleResp(resp, combineDownsampleQueryTable(planExecuteResults, aggregateType, groupByLevels));
    }

    public static void combineDownsampleAvgQueryResult(DownsampleQueryResp resp, List<PlanExecuteResult> planExecuteResults, List<Integer> groupByLevels) throws ExecutionException {
        boolean needGroup = groupByLevels != null && !groupByLevels.isEmpty();
        if (!needGroup) {
            DownsampleCombiner.combineDownsampleQueryResult(resp, planExecuteResults, AggregateType.AVG, null);
            return;
        }
        List<PlanExecuteResult> sumPlanExecuteResults = planExecuteResults.stream()
                .filter(e -> e.getPlan().getIginxPlanType() == IginxPlan.IginxPlanType.SUM || e.getPlan().getIginxPlanType() == IginxPlan.IginxPlanType.DOWNSAMPLE_SUM)
                .collect(Collectors.toList());
        List<PlanExecuteResult> countPlanExecuteResult = planExecuteResults.stream()
                .filter(e -> e.getPlan().getIginxPlanType() == IginxPlan.IginxPlanType.COUNT || e.getPlan().getIginxPlanType() == IginxPlan.IginxPlanType.DOWNSAMPLE_COUNT)
                .collect(Collectors.toList());
        Table sumTable = combineDownsampleQueryTable(sumPlanExecuteResults, AggregateType.SUM, groupByLevels);
        Table countTable = combineDownsampleQueryTable(countPlanExecuteResult, AggregateType.COUNT, groupByLevels);

        // 使用 sumTable 和 countTable 计算 avgTable
        List<DataType> avgDataTypes = new ArrayList<>();
        List<String> avgPaths = new ArrayList<>(sumTable.columnNameList);
        Map<String, Integer> countPathIndices = new HashMap<>();
        for (int i = 0; i < countTable.columnNameList.size(); i++) {
            countPathIndices.put(countTable.columnNameList.get(i), i);
        }
        for (DataType dataType: sumTable.columnTypeList) {
            if (dataType != DataType.FLOAT) {
                avgDataTypes.add(DataType.DOUBLE);
            } else {
                avgDataTypes.add(DataType.FLOAT);
            }
        }
        List<Long> avgTimestamps = new ArrayList<>();

        List<Object[]> avgValuesList = new ArrayList<>();
        for (int i = 0; i < sumTable.valuesList.size(); i++) {
            Object[] sums = sumTable.valuesList.get(i);
            Object[] counts = countTable.valuesList.get(i);
            Object[] avgs = new Object[avgPaths.size()];
            for (int j = 0; j < sums.length; j++) {
                if (sums[j] == null) {
                    continue;
                }
                String path = avgPaths.get(j);
                int countIndex = countPathIndices.get(path);
                Object avg = null;
                Object sum = sums[j];
                long count = (long) counts[countIndex];
                if (count != 0) {
                    switch (sumTable.columnTypeList.get(j)) {
                        case INTEGER:
                            avg = (int) sum * 1.0 / count;
                            break;
                        case LONG:
                            avg = (long) sum * 1.0 / count;
                            break;
                        case FLOAT:
                            avg = (float) sum / count;
                            break;
                        case DOUBLE:
                            avg = (double) sum / count;
                            break;
                    }
                }
                avgs[j] = avg;
            }
            boolean allNull = true;
            for (Object avg: avgs) {
                if (avg != null) {
                    allNull = false;
                    break;
                }
            }
            if (!allNull) {
                avgTimestamps.add(sumTable.timestamps.get(i));
                avgValuesList.add(avgs);
            }
        }
        Table avgTable = new Table(avgPaths, avgDataTypes, avgTimestamps, avgValuesList);
        constructDownsampleResp(resp, avgTable);
    }

    static class Table {

        final List<String> columnNameList;

        final List<DataType> columnTypeList;

        final List<Long> timestamps;

        final List<Object[]> valuesList;

        Table(List<String> columnNameList, List<DataType> columnTypeList, List<Long> timestamps, List<Object[]> valuesList) {
            this.columnNameList = columnNameList;
            this.columnTypeList = columnTypeList;
            this.timestamps = timestamps;
            this.valuesList = valuesList;
        }
    }

    private static Table combineResult(List<QueryExecuteDataSet> queryExecuteDataSets) throws ExecutionException {
        Set<QueryExecuteDataSetWrapper> dataSetWrappers = queryExecuteDataSets.stream()
                .map(CheckedFunction.wrap(QueryExecuteDataSetWrapper::new))
                .collect(Collectors.toSet());
        List<String> columnNameList = new ArrayList<>();
        List<DataType> columnTypeList = new ArrayList<>();
        List<List<QueryExecuteDataSetWrapper>> columnSourcesList = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<Object[]> valuesList = new ArrayList<>();
        // 从序列名到 column 列表位置的映射，同时也负责检测某个列是否已经加入到列表中
        Map<String, Integer> columnPositionMap = new HashMap<>();
        // 初始化列集合
        for (QueryExecuteDataSetWrapper dataSetWrapper : dataSetWrappers) {
            List<String> columnNameSubList = dataSetWrapper.getColumnNames();
            List<DataType> columnTypeSubList = dataSetWrapper.getColumnTypes();
            for (int i = 0; i < columnNameSubList.size(); i++) {
                String columnName = columnNameSubList.get(i);
                DataType columnType = columnTypeSubList.get(i);
                if (!columnPositionMap.containsKey(columnName)) {
                    columnPositionMap.put(columnName, columnNameList.size());
                    columnNameList.add(columnName);
                    columnTypeList.add(columnType);
                    columnSourcesList.add(new ArrayList<>());
                }
                columnSourcesList.get(columnPositionMap.get(columnName)).add(dataSetWrapper);
            }
        }
        // 初始化各个数据源
        // 在加载完一轮数据之后，把更新加载过数据的时间
        {
            Iterator<QueryExecuteDataSetWrapper> it = dataSetWrappers.iterator();
            Set<QueryExecuteDataSetWrapper> deletedDataSetWrappers = new HashSet<>();
            while(it.hasNext()) {
                QueryExecuteDataSetWrapper dataSetWrapper = it.next();
                if (dataSetWrapper.hasNext()) {
                    dataSetWrapper.next();
                } else { // 如果没有下一行，应该把当前数据集给移除掉
                    dataSetWrapper.close();
                    deletedDataSetWrappers.add(dataSetWrapper);
                    it.remove();
                }
            }
            // 删除掉已经空的 data source
            for (QueryExecuteDataSetWrapper dataSetWrapper : deletedDataSetWrappers) {
                List<String> columnNames = dataSetWrapper.getColumnNames();
                for (String columnName : columnNames) {
                    int index = columnPositionMap.get(columnName);
                    columnSourcesList.get(index).remove(dataSetWrapper);
                }
            }
        }

        while(!dataSetWrappers.isEmpty()) {
            long timestamp = Long.MAX_VALUE;
            // 顺序访问所有的还有数据数据的 timestamp，获取当前的时间戳
            for (QueryExecuteDataSetWrapper dataSetWrapper : dataSetWrappers) {
                timestamp = Math.min(dataSetWrapper.getTimestamp(), timestamp);
            }
            timestamps.add(timestamp);
            // 当前的行对应的数据
            Object[] values = new Object[columnTypeList.size()];
            for (int i = 0; i < columnTypeList.size(); i++) {
                String columnName = columnNameList.get(i);
                List<QueryExecuteDataSetWrapper> columnSources = columnSourcesList.get(i);
                for (QueryExecuteDataSetWrapper dataSetWrapper : columnSources) {
                    if (dataSetWrapper.getTimestamp() == timestamp) {
                        // 在检查某个字段时候，发现了时间戳符合的数据
                        Object value = dataSetWrapper.getValue(columnName);
                        if (value != null) {
                            // 时间戳符合更新 bitmap
                            values[i] = value;
                            break;
                        }
                    }
                }
            }
            valuesList.add(values);
            // 在加载完一轮数据之后，把更新加载过数据的时间
            Iterator<QueryExecuteDataSetWrapper> it = dataSetWrappers.iterator();
            Set<QueryExecuteDataSetWrapper> deletedDataSetWrappers = new HashSet<>();
            while(it.hasNext()) {
                QueryExecuteDataSetWrapper dataSetWrapper = it.next();
                if (dataSetWrapper.getTimestamp() == timestamp) { // 如果时间戳是当前的时间戳，则意味着本行读完了，加载下一行
                    if (dataSetWrapper.hasNext()) {
                        dataSetWrapper.next();
                    } else { // 如果没有下一行，应该把当前数据集给移除掉
                        dataSetWrapper.close();
                        deletedDataSetWrappers.add(dataSetWrapper);
                        it.remove();
                    }
                }
            }
            // 删除掉已经空的 data source
            for (QueryExecuteDataSetWrapper dataSetWrapper : deletedDataSetWrappers) {
                List<String> columnNames = dataSetWrapper.getColumnNames();
                for (String columnName : columnNames) {
                    int index = columnPositionMap.get(columnName);
                    columnSourcesList.get(index).remove(dataSetWrapper);
                }
            }
        }
        return new Table(columnNameList, columnTypeList, timestamps, valuesList);
    }

    private static Table groupTable(Table table, List<Integer> groupByLevels, AggregateType aggregateType) {
        List<String> paths = table.columnNameList;
        List<DataType> dataTypeList = table.columnTypeList;

        Map<String, DataType> dataTypeMap = new HashMap<>();
        Map<String, List<Integer>> reverseTransformMap = new HashMap<>();
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            DataType dataType = dataTypeList.get(i);
            String transformedPath = GroupByUtils.transformPath(path, groupByLevels);
            if (dataTypeMap.getOrDefault(transformedPath, dataType) != dataType) {
                throw new IllegalStateException("unexpected data type for " + path +  " in group by level.");
            }
            dataTypeMap.put(transformedPath, dataType);
            // 构造 reverseTransformMap
            List<Integer> reverseList = reverseTransformMap.getOrDefault(transformedPath, new ArrayList<>());
            reverseList.add(i);
            reverseTransformMap.put(transformedPath, reverseList);
        }
        List<String> transformedPaths = new ArrayList<>();
        List<DataType> transformedTypes = new ArrayList<>();
        for (Map.Entry<String, DataType> entry: dataTypeMap.entrySet()) {
            transformedPaths.add(entry.getKey());
            transformedTypes.add(entry.getValue());
        }

        List<Object[]> valuesList = new ArrayList<>();
        for (Object[] rawValues: table.valuesList) {
            Object[] values = new Object[transformedPaths.size()];
            for (int i = 0; i < transformedPaths.size(); i++) {
                String transformedPath = transformedPaths.get(i);
                DataType dataType = transformedTypes.get(i);
                List<Integer> rawPathIndices = reverseTransformMap.get(transformedPath);

                List<Object> correspondingValues = new ArrayList<>();
                for (int rawPathIndex: rawPathIndices) {
                    if (rawValues[rawPathIndex] != null) {
                        correspondingValues.add(rawValues[rawPathIndex]);
                    }
                }
                if (!correspondingValues.isEmpty()) {
                    values[i] = groupValues(correspondingValues, dataType, aggregateType);
                }
            }
            valuesList.add(values);
        }


        return new Table(transformedPaths, transformedTypes, table.timestamps, valuesList);
    }

    private static Object groupValues(List<Object> values, DataType dataType, AggregateType aggregateType) {
        if (aggregateType != AggregateType.SUM && aggregateType != AggregateType.COUNT) {
            throw new IllegalStateException();
        }
        Object result = null;
        switch (dataType) {
            case FLOAT:
                result = values.stream().map(Float.class::cast).reduce(Float::sum).orElse(0.0f);
                break;
            case DOUBLE:
                result = values.stream().map(Double.class::cast).reduce(Double::sum).orElse(0.0);
                break;
            case INTEGER:
                result = values.stream().map(Integer.class::cast).reduce(Integer::sum).orElse(0);
                break;
            case LONG:
                result = values.stream().map(Long.class::cast).reduce(Long::sum).orElse(0L);
                break;
            default:
                logger.error("unexpected type " + dataType);
        }
        return result;
    }

    private static void constructDownsampleResp(DownsampleQueryResp resp, Table table) {
        resp.setPaths(table.columnNameList);
        resp.setDataTypeList(table.columnTypeList);

        List<ByteBuffer> valuesList = new ArrayList<>();
        List<ByteBuffer> bitmapList = new ArrayList<>();
        for (Object[] values: table.valuesList) {
            Bitmap bitmap = new Bitmap(table.columnTypeList.size());
            for (int i = 0; i < values.length; i++) {
                if (values[i] != null) {
                    bitmap.mark(i);
                }
            }
            ByteBuffer buffer = ByteUtils.getRowByteBuffer(values, table.columnTypeList);
            valuesList.add(buffer);
            bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
        }
        resp.setQueryDataSet(new QueryDataSet(ByteUtils.getColumnByteBuffer(table.timestamps.toArray(), DataType.LONG),
                valuesList, bitmapList));
    }

}
