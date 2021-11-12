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
package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.List;

public class QueryAggregatorCurvematching extends QueryAggregator {
    public QueryAggregatorCurvematching() {
        super(QueryAggregatorType.CURVEMATCHING);
    }


    @Override
    public QueryResultDataset doAggregate(RestSession session, List<String> paths, long startTimestamp, long endTimestamp) {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try {
            SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startTimestamp, endTimestamp);
            queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
            DataType type = RestUtils.checkType(sessionQueryDataSet);
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            double bestResult = Double.MAX_VALUE;
            Long answerTimestamp = 0L;
            String answer = "";
            List<Double> queryList = RestUtils.norm(RestUtils.calcShapePattern(getCurveQuery(), true,true, true, 0.1, 0.05));
            Integer maxWarpingWindow = queryList.size() / 4;
            List<Double> upper = RestUtils.getWindow(queryList, maxWarpingWindow, true);
            List<Double> lower = RestUtils.getWindow(queryList, maxWarpingWindow, false);

            switch (type) {
                case LONG:
                case DOUBLE:
                    for (int j = 0; j < m; j++) {
                        List<Long> timestamps = new ArrayList<>();
                        List<Double> value = new ArrayList<>();
                        for (int i = 0; i < n; i++) {
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                                timestamps.add(sessionQueryDataSet.getTimestamps()[i]);
                                value.add((double) sessionQueryDataSet.getValues().get(i).get(j));
                            }
                        }
                        List<Double> valueList = RestUtils.calcShapePattern(RestUtils.transToNorm(timestamps, value, getCurveUnit()),
                                true, true, true,0.1, 0.05);
                        for (int i = 0; i < valueList.size() - queryList.size(); i++) {
                            double result = RestUtils.calcDTW(queryList, valueList.subList(i, i + queryList.size()), maxWarpingWindow, bestResult, upper, lower);
                            if (result < bestResult) {
                                bestResult = result;
                                answerTimestamp = timestamps.get(i);
                                answer = paths.get(j);
                            }
                        }
                    }
                    queryResultDataset.add(answerTimestamp, answer);
                    break;
                default:
                    throw new Exception("Unsupported data type");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryResultDataset;
    }
}
