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
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class QueryDataSetCombiner {

    private static final QueryDataSetCombiner instance = new QueryDataSetCombiner();

    private QueryDataSetCombiner() {}

    public QueryDataSet combine(QueryDataReq queryDataReq, List<QueryDataPlanExecuteResult> executeResults) {
        Map<String, List<TimeSeriesDataSet>>  tsSliceMap = executeResults.stream().filter(
                e -> e.getStatusCode() == QueryDataPlanExecuteResult.SUCCESS)
                .map(QueryDataPlanExecuteResult::getTimeSeriesDataSets)
                .flatMap(List<TimeSeriesDataSet>::stream)
                .collect(Collectors.groupingBy(TimeSeriesDataSet::getName));
        List<String> paths = new ArrayList<>();
        List<Pair<TimeSeriesDataSet, Integer>> tsList = new ArrayList<>();
        for (String path: tsSliceMap.keySet()) {
            TimeSeriesDataSet ts = mergeSlices(tsSliceMap.get(path));
            if (ts == null || ts.length() == 0)
                continue;
            paths.add(path);
            tsList.add(new Pair<>(ts, 0));
        }
        QueryDataSet dataSet = new QueryDataSet();
        List<Long> timeLine = new ArrayList<>();
        boolean noMoreTs = false;
        while (!noMoreTs) {
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
                noMoreTs = true;
            }
            timeLine.add(time);
            Bitmap bitmap = new Bitmap(paths.size());
            for (int i = 0; i < paths.size(); i++) {
                Pair<TimeSeriesDataSet, Integer> tsPair = tsList.get(i);
                if (tsPair.v != tsPair.k.length() && time == tsPair.k.getTime(tsPair.v)) {
                    bitmap.mark(i);

                }
            }
        }
        return null;
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
