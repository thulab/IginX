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

public class QueryAggregatorFilter extends QueryAggregator {
    public QueryAggregatorFilter() {
        super(QueryAggregatorType.FILTER);
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
                    for (int i = 0; i < n; i++) {
                        Long now = null;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                                if (now == null && filted((long) sessionQueryDataSet.getValues().get(i).get(j), getFilter()))
                                    now = (long) sessionQueryDataSet.getValues().get(i).get(j);
                                datapoints += 1;
                            }
                        if (now != null)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], now);
                    }
                    queryResultDataset.setSampleSize(datapoints);
                    break;
                case DOUBLE:
                    for (int i = 0; i < n; i++) {
                        Double nowd = null;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                                if (nowd == null && filted((double) sessionQueryDataSet.getValues().get(i).get(j), getFilter()))
                                    nowd = (double) sessionQueryDataSet.getValues().get(i).get(j);
                                datapoints += 1;
                            }
                        if (nowd != null)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], nowd);
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

    boolean filted(double now, Filter filter) {
        switch (filter.getOp()) {
            case "gt":
                if (now > filter.getValue()) return true;
                else return false;
            case "gte":
                if (now >= filter.getValue()) return true;
                else return false;
            case "eq":
                if (now == filter.getValue()) return true;
                else return false;
            case "ne":
                if (now != filter.getValue()) return true;
                else return false;
            case "lte":
                if (now <= filter.getValue()) return true;
                else return false;
            case "lt":
                if (now < filter.getValue()) return true;
                else return false;
            default:
                return false;
        }
    }
}
