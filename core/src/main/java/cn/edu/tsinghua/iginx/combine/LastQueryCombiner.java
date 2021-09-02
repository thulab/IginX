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

import cn.edu.tsinghua.iginx.combine.valuefilter.ValueFilterCombiner;
import cn.edu.tsinghua.iginx.query.result.AggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.LastQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;
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

public class LastQueryCombiner {

    private static final Logger logger = LoggerFactory.getLogger(ValueFilterCombiner.class);

    private static final LastQueryCombiner instance = new LastQueryCombiner();

    private LastQueryCombiner() {
    }

    public static LastQueryCombiner getInstance() {
        return instance;
    }

    public void combineResult(LastQueryResp resp, List<LastQueryPlanExecuteResult> planExecuteResults) {
        constructPathAndTypeList(resp, planExecuteResults);
        List<String> paths = resp.paths;
        List<DataType> dataTypes = resp.dataTypeList;
        Long[] times = new Long[paths.size()];
        Object[] values = new Object[paths.size()];
        Map<String, List<Pair<Long, Object>>> pathsRawData = new HashMap<>();
        for (LastQueryPlanExecuteResult planExecuteResult : planExecuteResults) {
            List<Object> subPathsRawData = planExecuteResult.getValues();
            List<Long> subTimes = planExecuteResult.getTimes();
            List<String> subPaths = planExecuteResult.getPaths();
            for (int i = 0; i < subPaths.size(); i++) {
                pathsRawData.computeIfAbsent(subPaths.get(i), e -> new ArrayList<>())
                        .add(new Pair<>(subTimes.get(i), subPathsRawData.get(i)));
            }
        }
        for (int i = 0; i < paths.size(); i++) {
            List<Pair<Long, Object>> rawData = pathsRawData.get(paths.get(i));
            if (rawData.isEmpty()) {
                continue;
            }
            Pair<Long, Object> lastPair = rawData.get(0);
            for (Pair<Long, Object> pair: rawData) {
                if (pair.v != null && (lastPair.v == null || pair.k > lastPair.k))
                    lastPair = pair;
            }
            times[i] = lastPair.k;
            values[i] = lastPair.v;
        }
        resp.timestamps = ByteUtils.getByteBufferFromLongArray(times);
        resp.valuesList = ByteUtils.getRowByteBuffer(values, dataTypes);
    }

    private void constructPathAndTypeList(LastQueryResp resp, List<LastQueryPlanExecuteResult> planExecuteResults) {
        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        Set<String> pathSet = new HashSet<>();
        for (LastQueryPlanExecuteResult planExecuteResult : planExecuteResults) {
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

}
