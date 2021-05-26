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
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;

import java.util.ArrayList;
import java.util.List;

public abstract class QueryAggregator {
    private Double divisor;
    private Long Dur;
    private double Percentile;
    private long Unit;
    private String metric_name;
    private Filter filter;
    private QueryAggregatorType type;


    protected QueryAggregator(QueryAggregatorType type) {
        this.type = type;
    }

    public Double getDivisor() {
        return divisor;
    }

    public void setDivisor(Double divisor) {
        this.divisor = divisor;
    }

    public Long getDur() {
        return Dur;
    }

    public void setDur(Long dur) {
        Dur = dur;
    }

    public double getPercentile() {
        return Percentile;
    }

    public void setPercentile(double percentile) {
        Percentile = percentile;
    }

    public long getUnit() {
        return Unit;
    }

    public void setUnit(long unit) {
        Unit = unit;
    }

    public String getMetric_name() {
        return metric_name;
    }

    public void setMetric_name(String metric_name) {
        this.metric_name = metric_name;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public QueryAggregatorType getType() {
        return type;
    }

    public void setType(QueryAggregatorType type) {
        this.type = type;
    }

    public QueryResultDataset doAggregate(RestSession session, List<String> paths, long startTimestamp, long endTimestamp) {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startTimestamp, endTimestamp);
        queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
        int n = sessionQueryDataSet.getTimestamps().length;
        int m = sessionQueryDataSet.getPaths().size();
        int datapoints = 0;
        for (int i = 0; i < n; i++) {
            boolean flag = false;
            for (int j = 0; j < m; j++)
                if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                    if (!flag) {
                        queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], sessionQueryDataSet.getValues().get(i).get(j));
                        flag = true;
                    }
                    datapoints += 1;
                }
        }
        queryResultDataset.setSampleSize(datapoints);
        return queryResultDataset;
    }

    public List<String> getPathsFromSessionQueryDataSet(SessionQueryDataSet sessionQueryDataSet) {
        List<String> ret = new ArrayList<>();
        List<Boolean> notNull = new ArrayList<>();
        int n = sessionQueryDataSet.getTimestamps().length;
        int m = sessionQueryDataSet.getPaths().size();
        for (int i = 0; i < m; i++)
            notNull.add(false);
        for (int i = 0; i < n; i++)
            for (int j = 0; j < m; j++) {
                if (sessionQueryDataSet.getValues().get(i).get(j) != null) {
                    notNull.set(j, true);
                }
            }
        for (int i = 0; i < m; i++)
            if (notNull.get(i))
                ret.add(sessionQueryDataSet.getPaths().get(i));
        return ret;
    }
}
