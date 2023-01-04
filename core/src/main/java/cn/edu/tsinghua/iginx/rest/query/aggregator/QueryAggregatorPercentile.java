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
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class QueryAggregatorPercentile extends QueryAggregator {
    public QueryAggregatorPercentile() {
        super(QueryAggregatorType.PERCENTILE);
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
            int datapoints = 0;
            switch (type) {
                case LONG:
                    List<Long> tmp = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        for (int j = 0; j < m; j++) {
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                                datapoints += 1;
                                tmp.add((long) sessionQueryDataSet.getValues().get(i).get(j));
                            }
                        }
                        if (i == n - 1 || RestUtils.getInterval(sessionQueryDataSet.getKeys()[i], startTimestamp, getDur()) !=
                            RestUtils.getInterval(sessionQueryDataSet.getKeys()[i + 1], startTimestamp, getDur())) {
                            Collections.sort(tmp);
                            queryResultDataset.add(RestUtils.getIntervalStart(sessionQueryDataSet.getKeys()[i], startTimestamp, getDur()),
                                tmp.get((int) Math.floor(getPercentile() * (tmp.size() - 1))));
                            tmp = new ArrayList<>();
                        }
                    }
                    break;
                case DOUBLE:
                    List<Double> tmpd = new ArrayList<>();
                    for (int i = 0; i < n; i++) {
                        for (int j = 0; j < m; j++) {
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                                datapoints += 1;
                                tmpd.add((double) sessionQueryDataSet.getValues().get(i).get(j));
                            }
                        }
                        if (i == n - 1 || RestUtils.getInterval(sessionQueryDataSet.getKeys()[i], startTimestamp, getDur()) !=
                            RestUtils.getInterval(sessionQueryDataSet.getKeys()[i + 1], startTimestamp, getDur())) {
                            Collections.sort(tmpd);
                            queryResultDataset.add(RestUtils.getIntervalStart(sessionQueryDataSet.getKeys()[i], startTimestamp, getDur()),
                                tmpd.get((int) Math.floor(getPercentile() * (tmpd.size() - 1))));
                            tmpd = new ArrayList<>();
                        }
                    }
                    break;
                default:
                    throw new Exception("Unsupported data type");
            }
            queryResultDataset.setSampleSize(datapoints);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queryResultDataset;
    }
}
