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
package cn.edu.tsinghua.iginx.combine;

import cn.edu.tsinghua.iginx.query.entity.TimeSeriesDataSet;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.booleanToByte;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteBufferFromTimestamps;

public class QueryDataSetCombiner {

    private static final Logger logger = LoggerFactory.getLogger(QueryDataSetCombiner.class);

    private static final QueryDataSetCombiner instance = new QueryDataSetCombiner();

    private QueryDataSetCombiner() {}

    public QueryDataResp combine(QueryDataReq queryDataReq, List<QueryDataPlanExecuteResult> executeResults, Status status) {
        Map<String, List<TimeSeriesDataSet>>  tsSliceMap = executeResults.stream().filter(
                e -> e.getStatusCode() == QueryDataPlanExecuteResult.SUCCESS)
                .map(QueryDataPlanExecuteResult::getTimeSeriesDataSets)
                .flatMap(List<TimeSeriesDataSet>::stream)
                .collect(Collectors.groupingBy(TimeSeriesDataSet::getName));
        List<String> paths = new ArrayList<>();
        List<DataType> types = new ArrayList<>();
        List<Pair<TimeSeriesDataSet, Integer>> tsList = new ArrayList<>();
        for (String path: tsSliceMap.keySet()) {
            TimeSeriesDataSet ts = mergeSlices(tsSliceMap.get(path));
            if (ts == null || ts.length() == 0)
                continue;
            paths.add(path);
            tsList.add(new Pair<>(ts, 0));
            types.add(ts.getType());
        }
        QueryDataSet dataSet = new QueryDataSet();
        List<Long> timeLine = new ArrayList<>();
        List<ByteBuffer> valueList = new ArrayList<>();
        List<ByteBuffer> bitmapList = new ArrayList<>();
        while (true) {
            long time = -1;
            // 找出最小时间
            for (int i = 0; i < paths.size(); i++) {
                Pair<TimeSeriesDataSet, Integer> tsPair = tsList.get(i);
                if (tsPair.v == tsPair.k.length()) // 当前时间序列已经处理完了
                    continue;
                if (time == -1 || time < tsPair.k.getTime(tsPair.v))
                    time = tsPair.k.getTime(tsPair.v);
            }
            if (time == -1) {
                break;
            }
            timeLine.add(time);
            List<Pair<Object, DataType>> data = new ArrayList<>();
            int totalSize = 0;
            Bitmap bitmap = new Bitmap(paths.size());
            for (int i = 0; i < paths.size(); i++) {
                Pair<TimeSeriesDataSet, Integer> tsPair = tsList.get(i);
                if (tsPair.v != tsPair.k.length() && time == tsPair.k.getTime(tsPair.v)) {
                    bitmap.mark(i);
                    data.add(new Pair<>(tsPair.k.getValue(tsPair.v), tsPair.k.getType()));
                    switch (tsPair.k.getType()) {
                        case FLOAT:
                        case INTEGER:
                            totalSize += 4;
                            break;
                        case DOUBLE:
                        case LONG:
                            totalSize += 8;
                            break;
                        case BOOLEAN:
                            totalSize += 1;
                            break;
                        default:
                            logger.error("unsupported datatype: " + tsPair.k.getType());
                    }
                    tsPair.v++;
                }
            }
            ByteBuffer buffer = ByteBuffer.allocate(totalSize);
            for (Pair<Object, DataType> datum: data) {
                switch (datum.v) {
                    case INTEGER:
                        buffer.putInt((int)datum.k);
                        break;
                    case LONG:
                        buffer.putLong((long)datum.k);
                        break;
                    case FLOAT:
                        buffer.putFloat((float) datum.k);
                        break;
                    case DOUBLE:
                        buffer.putDouble((double) datum.k);
                        break;
                    case BOOLEAN:
                        buffer.put(booleanToByte((boolean) datum.k));
                        break;
                    default:
                        logger.error("unsupported data type: " + datum.v);
                }
            }
            valueList.add(buffer);
            byte[] bitmapValue = bitmap.getBytes();
            ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapValue.length);
            bitmapBuffer.put(bitmapBuffer);
            bitmapList.add(bitmapBuffer);
        }
        dataSet.setBitmapList(bitmapList);
        dataSet.setValuesList(valueList);
        dataSet.setTimestamps(getByteBufferFromTimestamps(timeLine));
        QueryDataResp resp = new QueryDataResp(status);
        resp.setQueryDataSet(dataSet);
        resp.setPaths(paths);
        resp.setDataTypeList(types);
        return resp;
    }

    private TimeSeriesDataSet mergeSlices(List<TimeSeriesDataSet> slices) {
        if (slices.size() == 0)
            return null;
        TimeSeriesDataSet ts = new TimeSeriesDataSet(slices.get(0).getName(), slices.get(0).getType());
        slices = slices.stream().filter(e -> e.length() == 0).collect(Collectors.toList());
        slices.sort((o1, o2) -> {
            long diff = o1.getBeginTime() - o2.getBeginTime();
            if (diff < 0)
                return -1;
            else if (diff > 0)
                return 1;
            else
                return 0;
        });
        for (int i = 0; i < slices.size(); i++) {
            TimeSeriesDataSet slice = slices.get(i);
            for (int j = 0; j < slice.length(); j++) {
                long time = slice.getTime(j);
                Object value = slice.getValue(j);
                ts.addDataPoint(time, value);
            }
        }
        return ts;
    }

    public static QueryDataSetCombiner getInstance() {
        return instance;
    }

}
