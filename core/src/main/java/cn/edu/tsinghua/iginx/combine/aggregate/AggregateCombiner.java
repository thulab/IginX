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
package cn.edu.tsinghua.iginx.combine.aggregate;

import cn.edu.tsinghua.iginx.query.result.AggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class AggregateCombiner {

    private static final Logger logger = LoggerFactory.getLogger(AggregateCombiner.class);

    private static final AggregateCombiner instance = new AggregateCombiner();

    private int compare(Object o1, Object o2, DataType dataType) {
        switch (dataType) {
            case INTEGER:
                int i1 = (int) o1, i2 = (int) o2;
                return Integer.compare(i1, i2);
            case LONG:
                long l1 = (long) o1, l2 = (long) o2;
                return Long.compare(l1, l2);
            case FLOAT:
                float f1 = (float) o1, f2 = (float) o2;
                return Float.compare(f1, f2);
            case DOUBLE:
                double d1 = (double) o1, d2 = (double) o2;
                return Double.compare(d1, d2);
        }
        return 0;
    }

    private void constructPathAndTypeList(AggregateQueryResp resp, List<? extends AggregateQueryPlanExecuteResult> planExecuteResults) {
        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        Set<String> pathSet = new HashSet<>();
        for (AggregateQueryPlanExecuteResult planExecuteResult: planExecuteResults) {
            List<String> pathSubList = planExecuteResult.getPaths();
            List<DataType> dataTypeSubList = planExecuteResult.getDataTypes();
            for (int i = 0; i < pathSubList.size(); i++) {
                String path = pathSubList.get(i);
                DataType dataType = dataTypeSubList.get(i);
                if (!pathSet.contains(path)) {
                    pathSet.add(path);
                    paths.add(path);
                    dataTypeList.add(dataType);
                }
            }
        }
        resp.paths = paths;
        resp.dataTypeList = dataTypeList;
    }

    public void combineSumOrCountResult(AggregateQueryResp resp, List<StatisticsAggregateQueryPlanExecuteResult> planExecuteResults) {
        logger.debug("combine sum/count result, there are " + planExecuteResults.size() + " sub results.");
        constructPathAndTypeList(resp, planExecuteResults);
        List<String> paths = resp.paths;
        List<DataType> dataTypes = resp.dataTypeList;
        Map<String, List<Object>> pathsRawData = new HashMap<>();
        for (StatisticsAggregateQueryPlanExecuteResult planExecuteResult: planExecuteResults) {
            List<String> subPaths = planExecuteResult.getPaths();
            List<Object> subPathsRawData = planExecuteResult.getValues();
            for (int i = 0; i < subPaths.size(); i++) {
                pathsRawData.computeIfAbsent(subPaths.get(i), e -> new ArrayList<>())
                    .add(subPathsRawData.get(i));
            }
        }
        Object[] values = new Object[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            DataType dataType = dataTypes.get(i);
            List<Object> rawData = pathsRawData.get(path);
            switch (dataType) {
                case FLOAT:
                    values[i] = rawData.stream().map(Float.class::cast).reduce(Float::sum).orElse(0.0f);
                    break;
                case DOUBLE:
                    values[i] = rawData.stream().map(Double.class::cast).reduce(Double::sum).orElse(0.0);
                    break;
                case INTEGER:
                    values[i] = rawData.stream().map(Integer.class::cast).reduce(Integer::sum).orElse(0);
                    break;
                case LONG:
                    values[i] = rawData.stream().map(Long.class::cast).reduce(Long::sum).orElse(0L);
                    break;
                default:
                    logger.error("unexpected type " + dataType);
            }
        }
        resp.valuesList = ByteUtils.getByteBuffer(values, dataTypes);
    }

    public void combineFirstResult(AggregateQueryResp resp, List<SingleValueAggregateQueryPlanExecuteResult> planExecuteResults) {
        combineSingleValueResult(resp, planExecuteResults, e -> e.v.stream().reduce((x, y) -> x.k < y.k ? x : y).orElse(null));
    }

    public void combineLastResult(AggregateQueryResp resp, List<SingleValueAggregateQueryPlanExecuteResult> planExecuteResults) {
        combineSingleValueResult(resp, planExecuteResults, e -> e.v.stream().reduce((x, y) -> x.k > y.k ? x : y).orElse(null));
    }

    public void combineMinResult(AggregateQueryResp resp, List<SingleValueAggregateQueryPlanExecuteResult> planExecuteResults) {
        combineSingleValueResult(resp, planExecuteResults, e -> e.v.stream().reduce((x, y) -> {
            int compareResult = compare(x.v, y.v, e.k);
            if (compareResult < 0)
                return x;
            else if (compareResult > 0)
                return y;
            else if (x.k < y.k)
                return x;
            else
                return y;
        }).orElse(null));
    }

    public void combineMaxResult(AggregateQueryResp resp, List<SingleValueAggregateQueryPlanExecuteResult> planExecuteResults) {
        combineSingleValueResult(resp, planExecuteResults, e -> e.v.stream().reduce((x, y) -> {
            int compareResult = compare(x.v, y.v, e.k);
            if (compareResult > 0)
                return x;
            else if (compareResult < 0)
                return y;
            else if (x.k < y.k)
                return x;
            else
                return y;
        }).orElse(null));
    }

    private void combineSingleValueResult(AggregateQueryResp resp, List<SingleValueAggregateQueryPlanExecuteResult> planExecuteResults,
                                          Function<Pair<DataType, List<Pair<Long, Object>>>, Pair<Long, Object>> mapper) {
        constructPathAndTypeList(resp, planExecuteResults);
        List<String> paths = resp.paths;
        List<DataType> dataTypes = resp.dataTypeList;
        Long[] times = new Long[paths.size()];
        Object[] values = new Object[paths.size()];
        Map<String, List<Pair<Long, Object>>> pathsRawData = new HashMap<>();
        for (SingleValueAggregateQueryPlanExecuteResult planExecuteResult: planExecuteResults) {
            List<Object> subPathsRawData = planExecuteResult.getValues();
            List<Long> subTimes = planExecuteResult.getTimes();
            List<String> subPaths = planExecuteResult.getPaths();
            for (int i = 0; i < subPaths.size(); i++) {
                pathsRawData.computeIfAbsent(subPaths.get(i), e -> new ArrayList<>())
                        .add(new Pair<>(subTimes.get(i), subPathsRawData.get(i)));
            }
        }
        for (int i = 0; i < paths.size(); i++) {
            Pair<Long, Object> pair = mapper.apply(new Pair<>(dataTypes.get(i), pathsRawData.get(paths.get(i))));
            times[i] = pair.k;
            values[i] = pair.v;
        }
        resp.timestamps = ByteUtils.getByteBuffer(times);
        resp.valuesList = ByteUtils.getByteBuffer(values, dataTypes);
    }

    public void combineAvgResult(AggregateQueryResp resp, List<AvgAggregateQueryPlanExecuteResult> planExecuteResults) {
        constructPathAndTypeList(resp, planExecuteResults);
        List<String> paths = resp.paths;
        List<DataType> dataTypes = resp.dataTypeList;
        Map<String, Pair<DataType, List<Pair<Long, Object>>>> pathsRawData = new HashMap<>();
        for (AvgAggregateQueryPlanExecuteResult planExecuteResult: planExecuteResults) {
            List<String> subPaths = planExecuteResult.getPaths();
            List<DataType> subDataTypes = planExecuteResult.getDataTypes();
            List<Long> subCounts = planExecuteResult.getCounts();
            List<Object> subSums = planExecuteResult.getSums();
            for (int i = 0; i < subPaths.size(); i++) {
                DataType dataType = subDataTypes.get(i);
                Long subCount = subCounts.get(i);
                Object subSum = subSums.get(i);
                pathsRawData.computeIfAbsent(subPaths.get(i), e -> new Pair<>(dataType, new ArrayList<>()))
                        .v.add(new Pair<>(subCount, subSum));
            }
        }
        Object[] values = new Object[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            Pair<DataType, List<Pair<Long, Object>>> pair = pathsRawData.get(paths.get(i));
            long count = pair.v.stream().map(e -> e.k).reduce(0L, Long::sum);
            Object avg = null;
            switch (pair.k) {
                case INTEGER:
                    avg = pair.v.stream().map(e -> e.v).map(Integer.class::cast).reduce(0, Integer::sum)  / count;
                    dataTypes.set(i, DataType.LONG);
                    break;
                case LONG:
                    avg = pair.v.stream().map(e -> e.v).map(Long.class::cast).reduce(0L, Long::sum) / count;
                    break;
                case FLOAT:
                    avg = pair.v.stream().map(e -> e.v).map(Float.class::cast).reduce(0.0f, Float::sum) / count;
                    break;
                case DOUBLE:
                    avg = pair.v.stream().map(e -> e.v).map(Double.class::cast).reduce(0.0, Double::sum) / count;
                    break;
                default:
                    logger.error("unsupported datatype: " + pair.k);
            }
            values[i] = avg;
        }
        resp.valuesList = ByteUtils.getByteBuffer(values, dataTypes);
    }

    public static AggregateCombiner getInstance() {
        return instance;
    }

}
