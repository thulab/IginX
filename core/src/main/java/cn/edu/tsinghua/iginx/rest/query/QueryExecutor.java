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
package cn.edu.tsinghua.iginx.rest.query;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryMetric;
import cn.edu.tsinghua.iginx.rest.bean.QueryResult;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorNone;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryShowTimeSeries;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static cn.edu.tsinghua.iginx.rest.RestUtils.*;

public class QueryExecutor {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private Query query;

    private final RestSession session = new RestSession();

    public QueryExecutor(Query query) {
        this.query = query;
    }

    public QueryResult executeShowTimeSeries() throws Exception {
        QueryResult ret = new QueryResult();
        try {
            session.openSession();
            ret.addResultSet(new QueryShowTimeSeries().doAggregate(session));
            session.closeSession();
        } catch (Exception e) {
            LOGGER.error("Error occurred during executing", e);
            throw e;
        }
        return ret;
    }

    public QueryResult execute(boolean isDelete) throws Exception {
        QueryResult ret = new QueryResult();
        try {
            session.openSession();
            for (QueryMetric queryMetric : query.getQueryMetrics()) {
                List<String> paths = new ArrayList<>();
                StringBuilder path = new StringBuilder();
                path.append(queryMetric.getName());
                paths.add(path.toString());
                if (isDelete) {
                    RestSession session = new RestSession();
                    session.openSession();
                    session.deleteDataInColumns(paths, queryMetric.getTags(), query.getStartAbsolute(), query.getEndAbsolute(), query.getTimePrecision());
                    session.closeSession();
                } else if (queryMetric.getAggregators().size() == 0) {
                    ret.addResultSet(new QueryAggregatorNone().doAggregate(session, paths, queryMetric.getTags(), query.getStartAbsolute(), query.getEndAbsolute(), query.getTimePrecision()), queryMetric, new QueryAggregatorNone());
                } else {
                    for (QueryAggregator queryAggregator : queryMetric.getAggregators()) {
                        ret.addResultSet(queryAggregator.doAggregate(session, paths, queryMetric.getTags(), query.getStartAbsolute(), query.getEndAbsolute(), query.getTimePrecision()), queryMetric, queryAggregator);
                    }
                }
            }
            session.closeSession();
        } catch (Exception e) {
            LOGGER.error("Error occurred during executing", e);
            throw e;
        }

        return ret;
    }

    private String getStringFromObject(Object val) {
        String valStr = new String();
        if (val instanceof byte[]) {
            valStr = new String((byte[]) val);
        } else {
            valStr = String.valueOf(val.toString());
        }
        return valStr;
    }


    //结果通过引用传出
    public void queryAnno(QueryResult anno) throws Exception {
        QueryResult title = new QueryResult(), description = new QueryResult();
        Query titleQuery = new Query();
        Query descriptionQuery = new Query();
        boolean hasTitle = false,hasDescription = false;
        try {
            for(int i=0; i<anno.getQueryResultDatasets().size(); i++) {
                QueryResultDataset data = anno.getQueryResultDatasets().get(i);
                if(data.getTimeLists().isEmpty()) continue;
                int subLen = data.getTimeLists().size();
                for(int j=0; j<subLen; j++) {
                    List<Long> timeList = data.getTimeLists().get(j);
                    for(int z=timeList.size()-1; z>=0;z--) {
                        //这里减小了对时间查询的范围
                        if(timeList.get(z) < DESCRIPTIONTIEM) break;

                        //将多种类型转换为Long
                        Long annoTime = getLongVal(data.getValueLists().get(j).get(z));

                        if (timeList.get(z).equals(TITLETIEM)) {
                            hasTitle = true;
                            titleQuery.setStartAbsolute(annoTime);
                            titleQuery.setEndAbsolute(annoTime + 1L);
                        } else if (timeList.get(z).equals(DESCRIPTIONTIEM)) {
                            hasDescription = true;
                            descriptionQuery.setStartAbsolute(annoTime);
                            descriptionQuery.setEndAbsolute(annoTime + 1L);
                        }
                    }

                    QueryMetric metric = new QueryMetric();
                    metric.setName(ANNOTAIONSEQUENCE);
                    List<QueryMetric> metrics = new ArrayList<>();
                    metrics.add(metric);
                    if(hasTitle) {
                        titleQuery.setQueryMetrics(metrics);
                        titleQuery.setTimePrecision(TimePrecision.NS);
                        this.query = titleQuery;
                        title = execute(false);
                        anno.getQueryResultDatasets().get(i).addTitle(getStringFromObject(title.getQueryResultDatasets().get(0).getValues().get(0)));
                    } else {
                        anno.getQueryResultDatasets().get(i).addTitle(new String());
                    }
                    if(hasDescription) {
                        descriptionQuery.setQueryMetrics(metrics);
                        descriptionQuery.setTimePrecision(TimePrecision.NS);
                        this.query = descriptionQuery;
                        description = execute(false);
                        anno.getQueryResultDatasets().get(i).addDescription(getStringFromObject(description.getQueryResultDatasets().get(0).getValues().get(0)));
                    } else {
                        anno.getQueryResultDatasets().get(i).addDescription(new String());
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during executing", e);
            throw e;
        }
    }

    public void deleteMetric() throws Exception {
        RestSession restSession = new RestSession();
        restSession.openSession();
        List<String> ins = new ArrayList<>();
        for(QueryMetric metric : query.getQueryMetrics()) {
            ins.add(metric.getPathName());
        }
        if(!ins.isEmpty())
            restSession.deleteColumns(ins);
        restSession.closeSession();
    }

    public DataType judgeObjectType(Object obj){
        if (obj instanceof Boolean){
            return DataType.BOOLEAN;
        }else if (obj instanceof Byte || obj instanceof String || obj instanceof Character){
            return DataType.BINARY;
        }else if (obj instanceof Long || obj instanceof Integer){
            return DataType.LONG;
        }else if (obj instanceof Double || obj instanceof Float){
            return DataType.DOUBLE;
        }
        //否则默认字符串类型
        return DataType.BINARY;
    }

    //错误返回-1
    public Long getLongVal(Object val) {
        switch (judgeObjectType(val)) {
            case BINARY:
                return Long.valueOf(new String((byte[]) val));
            case DOUBLE:
                return Math.round((Double)(val));
            case LONG:
                return (Long)val;
            default:
                return new Long(-1);//尽量不要传null
        }
    }
}