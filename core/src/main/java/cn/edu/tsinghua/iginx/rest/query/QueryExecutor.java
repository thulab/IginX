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

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryMetric;
import cn.edu.tsinghua.iginx.rest.bean.QueryResult;
import cn.edu.tsinghua.iginx.rest.insert.DataPointsParser;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorNone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class QueryExecutor {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private final IMetaManager metaManager = DefaultMetaManager.getInstance();
    private final Query query;

    private final RestSession session = new RestSession();

    public QueryExecutor(Query query) {
        this.query = query;
    }

    public QueryResult execute(boolean isDelete) throws Exception {
        QueryResult ret = new QueryResult();
        try {
            session.openSession();
            for (QueryMetric queryMetric : query.getQueryMetrics()) {
                List<String> paths = new ArrayList<>();
                StringBuilder path = new StringBuilder();
                if (queryMetric.getAnnotation()) {
                    path.append(queryMetric.getName()).append(DataPointsParser.ANNOTATION_SPLIT_STRING);
                } else {
                    path.append(queryMetric.getName());
                }
                paths.add(path.toString());
                if (isDelete) {
                    RestSession session = new RestSession();
                    session.openSession();
                    session.deleteDataInColumns(paths, queryMetric.getTags(), query.getStartAbsolute(), query.getEndAbsolute());
                    session.closeSession();
                } else if (queryMetric.getAggregators().size() == 0) {
                    ret.addResultSet(new QueryAggregatorNone().doAggregate(session, paths, queryMetric.getTags(), query.getStartAbsolute(), query.getEndAbsolute()), queryMetric, new QueryAggregatorNone());
                } else {
                    for (QueryAggregator queryAggregator : queryMetric.getAggregators()) {
                        ret.addResultSet(queryAggregator.doAggregate(session, paths, queryMetric.getTags(), query.getStartAbsolute(), query.getEndAbsolute()), queryMetric, queryAggregator);
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

    public List<String> getPaths(QueryMetric queryMetric) throws Exception {
        List<String> ret = new ArrayList<>();
        Map<String, Integer> metricschema = metaManager.getSchemaMapping(queryMetric.getName());
        if (metricschema == null) {
            throw new Exception("No metadata found");
        } else {
            Map<Integer, String> pos2path = new TreeMap<>();
            for (Map.Entry<String, Integer> entry : metricschema.entrySet()) {
                pos2path.put(entry.getValue(), entry.getKey());
            }
            List<Integer> pos = new ArrayList<>();
            for (int i = 0; i < pos2path.size(); i++) {
                pos.add(-1);
            }
            searchPath(0, ret, pos2path, queryMetric, pos);
        }
        return ret;
    }

    void searchPath(int depth, List<String> paths, Map<Integer, String> pos2path, QueryMetric queryMetric, List<Integer> pos) {
        if (depth == pos2path.size()) {
            StringBuilder path = new StringBuilder();
            Iterator iter = pos2path.entrySet().iterator();
            int now = 0;
            if (queryMetric.getAnnotation()) {
                path.append(queryMetric.getName()).append(DataPointsParser.ANNOTATION_SPLIT_STRING);
            } else {
                path.append(queryMetric.getName());
            }
            paths.add(path.toString());
            return;
        }
        if (queryMetric.getTags().get(pos2path.get(depth + 1)) == null) {
            pos.set(depth, -1);
            searchPath(depth + 1, paths, pos2path, queryMetric, pos);
        } else {
            for (int i = 0; i < queryMetric.getTags().get(pos2path.get(depth + 1)).size(); i++) {
                pos.set(depth, i);
                searchPath(depth + 1, paths, pos2path, queryMetric, pos);
            }
        }
    }
}