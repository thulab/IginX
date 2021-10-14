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

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.insert.DataPointsParser;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorNone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class QueryExecutor {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private final IMetaManager metaManager = DefaultMetaManager.getInstance();
    private Query query;
    private Integer maxPathLength = 3;
    private RestSession session = new RestSession();


    public QueryExecutor(Query query) {
        this.query = query;
    }

    public QueryResult execute(boolean isDelete) throws Exception {
        QueryResult ret = new QueryResult();
        try {
            session.openSession();
            for (QueryMetric queryMetric : query.getQueryMetrics()) {
                List<String> paths = getPaths(queryMetric);
                if (isDelete) {
                    RestSession session = new RestSession();
                    session.openSession();
                    session.deleteDataInColumns(paths, query.getStartAbsolute(), query.getEndAbsolute());
                    session.closeSession();
                } else if (queryMetric.getAggregators().size() == 0) {
                    ret.addResultSet(new QueryAggregatorNone().doAggregate(session, paths, query.getStartAbsolute(), query.getEndAbsolute()), queryMetric, new QueryAggregatorNone());
                } else {
                    for (QueryAggregator queryAggregator : queryMetric.getAggregators()) {
                        ret.addResultSet(queryAggregator.doAggregate(session, paths, query.getStartAbsolute(), query.getEndAbsolute()), queryMetric, queryAggregator);
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
        Map<Integer, String> pos2path = new TreeMap<>();
        int now = 0;
        for (Map.Entry<String, List<String>> entry : queryMetric.getTags().entrySet()) {
            now ++;
            pos2path.put(now, entry.getKey());
        }
        List<Integer> pos = new ArrayList<>();
        for (int i = 0; i < maxPathLength; i++)
            pos.add(-1);
        dfsInsert(0, ret, pos2path, queryMetric, pos, 0);
        return ret;
    }

    void dfsInsert(int depth, List<String> Paths, Map<Integer, String> pos2path, QueryMetric queryMetric, List<Integer> pos, int nowPos) {
        if (nowPos == pos2path.size()) {
            StringBuilder path = new StringBuilder("");
            int tmpPos = 0;
            for (int i = 0; i < depth; i++) {
                String ins = null;
                List<String> tmp = null;
                if (pos.get(i) != -1) {
                    tmp = queryMetric.getTags().get(pos2path.get(tmpPos + 1));
                    tmpPos ++;
                }
                if (tmp != null) {
                    ins = tmp.get(pos.get(i));
                }
                if (ins != null) {
                    path.append(pos2path.get(tmpPos) + ".");
                    path.append(ins + ".");
                }else {
                    path.append("*.");
                    path.append("*.");
                }
            }
            path.append(queryMetric.getName());
            Paths.add(path.toString());
        }
        if (depth == maxPathLength) {
            return;
        }
        pos.set(depth, -1);
        dfsInsert(depth + 1, Paths, pos2path, queryMetric, pos, nowPos);
        if (pos2path.size() > nowPos)
            for (int i = 0; i < queryMetric.getTags().get(pos2path.get(nowPos + 1)).size(); i++) {
                pos.set(depth, i);
                dfsInsert(depth + 1, Paths, pos2path, queryMetric, pos, nowPos + 1);
            }

    }
}