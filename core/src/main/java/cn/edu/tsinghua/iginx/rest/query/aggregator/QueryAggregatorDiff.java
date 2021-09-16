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
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.List;

public class QueryAggregatorDiff extends QueryAggregator {

    public QueryAggregatorDiff() {
        super(QueryAggregatorType.DIFF);
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
            int datapoints = 0;
            switch (type) {
                case LONG:
                    Long last = null;
                    Long now = null;
                    for (int i = 0; i < n; i++) {
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                                if (now == null)
                                    now = (long) sessionQueryDataSet.getValues().get(i).get(j);
                                datapoints += 1;
                            }
                        if (i != 0) {
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], now - last);
                        }
                        last = now;
                        now = null;
                    }
                    queryResultDataset.setSampleSize(datapoints);
                    break;

                case DOUBLE:
                    Double lastd = null;
                    Double nowd = null;
                    for (int i = 0; i < n; i++) {
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                                if (nowd == null)
                                    nowd = (double) sessionQueryDataSet.getValues().get(i).get(j);
                                datapoints += 1;
                            }
                        if (i != 0) {
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], nowd - lastd);
                        }
                        lastd = nowd;
                        nowd = null;
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
