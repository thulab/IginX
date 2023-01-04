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

import java.util.List;
import java.util.Map;

public class QueryAggregatorDev extends QueryAggregator {
    public QueryAggregatorDev() {
        super(QueryAggregatorType.DEV);
    }

    @Override
    public QueryResultDataset doAggregate(RestSession session, List<String> paths, Map<String, List<String>> tagList, long startTimestamp, long endTimestamp) {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try {
            SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startTimestamp, endTimestamp, tagList);
            queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
            DataType type = RestUtils.checkType(sessionQueryDataSet);
            int n = sessionQueryDataSet.getKeys().length;
            int m = sessionQueryDataSet.getPaths().size();
            switch (type) {
                case LONG:
                case DOUBLE:
                    int datapoints = 0;
                    double sum = 0, sum2 = 0;
                    long cnt = 0;
                    for (int i = 0; i < n; i++) {
                        for (int j = 0; j < m; j++) {
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                                sum += (double) sessionQueryDataSet.getValues().get(i).get(j);
                                sum2 += Math.pow((double) sessionQueryDataSet.getValues().get(i).get(j), 2);
                                cnt += 1;
                                datapoints += 1;
                            }
                        }
                        if (i == n - 1 || RestUtils.getInterval(sessionQueryDataSet.getKeys()[i], startTimestamp, getDur()) !=
                            RestUtils.getInterval(sessionQueryDataSet.getKeys()[i + 1], startTimestamp, getDur())) {
                            queryResultDataset.add(RestUtils.getIntervalStart(sessionQueryDataSet.getKeys()[i], startTimestamp, getDur()),
                                sum2 / cnt - Math.pow(sum / cnt, 2));
                            sum = 0;
                            sum2 = 0;
                            cnt = 0;
                        }
                    }
                    queryResultDataset.setSampleSize(datapoints);
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
