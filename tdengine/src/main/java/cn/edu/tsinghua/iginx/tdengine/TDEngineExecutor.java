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
package cn.edu.tsinghua.iginx.tdengine;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DropDatabasePlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.ValueFilterQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.query.IStorageEngine;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DownsampleQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ValueFilterQueryPlanExecuteResult;

import com.alibaba.druid.pool.DruidDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.FAILURE;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.SUCCESS;

public class TDEngineExecutor implements IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(TDEngineExecutor.class);

    private static final String QUERY_DATA = "select * from %s where ts >= %d and ts < %d";

    private static final String CREATE_TABLE = "create table if not exists %s (ts timestamp, field0 binary(1030))";

    private static final String INSERT_DATA = "insert into %s values %s";

    private static final String INSERT_VALUE = "(%d, '%s') ";

    private static final Set<String> tables = Collections.synchronizedSet(new HashSet<>());

    private final Map<Long, DruidDataSource> connectionPools = new ConcurrentHashMap<>();

    public static boolean testConnection(StorageEngineMeta storageEngineMeta) {
        return true;
    }

    public TDEngineExecutor(List<StorageEngineMeta> storageEngineMetaList) {
        for (StorageEngineMeta meta: storageEngineMetaList) {
            String ip = meta.getIp();
            int port = meta.getPort();
            String db = meta.getExtraParams().getOrDefault("db", "tpc");
            int maxActive = Integer.parseInt(meta.getExtraParams().getOrDefault("maxActive", "50"));
            int minIdle = Integer.parseInt(meta.getExtraParams().getOrDefault("minIdle", "10"));
            int initialSize = Integer.parseInt(meta.getExtraParams().getOrDefault("initialSize", "10"));
            int maxWait = Integer.parseInt(meta.getExtraParams().getOrDefault("maxWait", "30000"));

            String conn = meta.getExtraParams().getOrDefault("conn", "rest");

            DruidDataSource dataSource = new DruidDataSource();
            dataSource.setDriverClassName(ConfigDescriptor.getInstance().getConfig().getTdEngineDriver());
            if (conn.equals("rest")) {
                dataSource.setUrl("jdbc:TAOS-RS://" + ip + ":" + port + "/" + db);
            } else {
                dataSource.setUrl("jdbc:TAOS://" + ip + ":" + port + "/" + db);
            }
            dataSource.setUsername("root");
            dataSource.setPassword("taosdata");
            dataSource.setInitialSize(initialSize);
            dataSource.setMinIdle(minIdle);
            dataSource.setMaxActive(maxActive);
            dataSource.setMaxWait(maxWait);

            connectionPools.put(meta.getId(), dataSource);
        }
    }

    private Connection getConnection(long id) throws SQLException {
        DruidDataSource dataSource = connectionPools.get(id);
        return dataSource.getConnection();
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan) {
        try (Connection conn = getConnection(plan.getStorageEngineId())) {
            List<StringBuilder> valueBuilders = new ArrayList<>();
            for (int i = 0; i < plan.getPathsNum(); i++) {
                valueBuilders.add(new StringBuilder());
            }
            for (int i = 0; i < plan.getTimestamps().length; i++) {
                int k = 0;
                for (int j = 0; j < plan.getPathsNum(); j++) {
                    if (plan.getBitmap(i).get(j)) {
                        valueBuilders.get(j).append(String.format(INSERT_VALUE, plan.getTimestamp(i), new String((byte[]) plan.getValues(i)[k])));
                        k++;
                    }
                }
            }

            Statement stmt = conn.createStatement();
            for (int i = 0; i < plan.getPathsNum(); i++) {
                String path = plan.getPath(i).replace('.', '_');
                String table = path.substring(0, path.lastIndexOf('_'));
                if (!tables.contains(table)) { // 数据表不存在，尝试建表
                    stmt.executeUpdate(String.format(CREATE_TABLE, table));
                    tables.add(table);
                }
                String value = valueBuilders.get(i).toString();
                stmt.executeUpdate(String.format(INSERT_DATA, table, value));
            }
            stmt.close();

        } catch (SQLException e) {
            logger.error("get error when insert: ", e);
            return new NonDataPlanExecuteResult(FAILURE, plan);
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        List<QueryExecuteDataSet> dataSets = new ArrayList<>();
        try (Connection conn = getConnection(plan.getStorageEngineId())) {
            for (String path: plan.getPaths()) {
                path = path.replace('.', '_');
                String table = path.substring(0, path.lastIndexOf('_'));

                Statement stmt = conn.createStatement();

                if (!tables.contains(table)) { // 如果表不存在，尝试创建表
                    stmt.executeUpdate(String.format(CREATE_TABLE, table));
                    tables.add(table);
                }

                ResultSet result = stmt.executeQuery(String.format(QUERY_DATA, table, plan.getStartTime(), plan.getEndTime()));
                // 加载出来数据
                dataSets.add(new TDEngineQueryExecuteDataSet(result));
                // 关闭查询结果集
                stmt.close();
            }
        } catch (SQLException | ParseException e) {
            logger.error("get error when query: ", e);
            return new QueryDataPlanExecuteResult(FAILURE, plan, null);
        }
        return new QueryDataPlanExecuteResult(SUCCESS, plan, dataSets);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan) {
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan) {
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan) {
        return null;
    }

    @Override
    public AvgAggregateQueryPlanExecuteResult syncExecuteAvgQueryPlan(AvgQueryPlan plan) {
        return null;
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteCountQueryPlan(CountQueryPlan plan) {
        return null;
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteSumQueryPlan(SumQueryPlan plan) {
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstQueryPlan(FirstQueryPlan plan) {
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan) {
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMaxQueryPlan(MaxQueryPlan plan) {
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMinQueryPlan(MinQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleAvgQueryPlan(DownsampleAvgQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleCountQueryPlan(DownsampleCountQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleSumQueryPlan(DownsampleSumQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMaxQueryPlan(DownsampleMaxQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMinQueryPlan(DownsampleMinQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstQueryPlan(DownsampleFirstQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastQueryPlan(DownsampleLastQueryPlan plan) {
        return null;
    }

    @Override
    public ValueFilterQueryPlanExecuteResult syncExecuteValueFilterQueryPlan(ValueFilterQueryPlan plan) {
        return null;
    }

    @Override
    public StorageEngineChangeHook getStorageEngineChangeHook() {
        return null;
    }
}
