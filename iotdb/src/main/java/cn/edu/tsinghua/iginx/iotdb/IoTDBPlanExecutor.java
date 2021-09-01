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
package cn.edu.tsinghua.iginx.iotdb;

import cn.edu.tsinghua.iginx.db.StorageEngine;
import cn.edu.tsinghua.iginx.iotdb.query.entity.IoTDBQueryExecuteDataSet;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.ShowColumnsPlan;
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
import cn.edu.tsinghua.iginx.query.result.ShowColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ValueFilterQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.fromIoTDB;
import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.toIoTDB;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.FAILURE;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.SUCCESS;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;
import static cn.edu.tsinghua.iginx.thrift.DataType.DOUBLE;
import static cn.edu.tsinghua.iginx.thrift.DataType.INTEGER;
import static cn.edu.tsinghua.iginx.thrift.DataType.LONG;

public class IoTDBPlanExecutor implements IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBPlanExecutor.class);

    private static final int BATCH_SIZE = 10000;

    private static final String PREFIX = "root.";

    private static final String TIME_RANGE_WHERE_CLAUSE = "WHERE time >= %d and time < %d";

    private static final String VALUE_FILTER_WHERE_CLAUSE = " and (%s)";

    private static final String GROUP_BY_CLAUSE = "GROUP BY ([%s, %s), %sms)";

    private static final String DELETE_CLAUSE = "DELETE FROM " + PREFIX + "%s";

    private static final String DELETE_TIMESERIES_CLAUSE = "DELETE TIMESERIES " + PREFIX + "%s";

    private static final String DELETE_STORAGE_GROUP_CLAUSE = "DELETE STORAGE GROUP " + PREFIX + "%s";

    private static final String QUERY_DATA = "SELECT %s FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String VALUE_FILTER_QUERY = "SELECT %s FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE + VALUE_FILTER_WHERE_CLAUSE;

    private static final String MAX_VALUE = "SELECT MAX_VALUE(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String MIN_VALUE = "SELECT MIN_VALUE(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String FIRST_VALUE = "SELECT FIRST_VALUE(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String LAST_VALUE = "SELECT LAST_VALUE(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String AVG = "SELECT COUNT(%s), SUM(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String COUNT = "SELECT COUNT(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String SUM = "SELECT SUM(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String MAX_VALUE_DOWNSAMPLE = "SELECT MAX_VALUE(%s) FROM " + PREFIX + "%s " + GROUP_BY_CLAUSE;

    private static final String MIN_VALUE_DOWNSAMPLE = "SELECT MIN_VALUE(%s) FROM " + PREFIX + "%s " + GROUP_BY_CLAUSE;

    private static final String FIRST_VALUE_DOWNSAMPLE = "SELECT FIRST_VALUE(%s) FROM " + PREFIX + "%s " + GROUP_BY_CLAUSE;

    private static final String LAST_VALUE_DOWNSAMPLE = "SELECT LAST_VALUE(%s) FROM " + PREFIX + "%s " + GROUP_BY_CLAUSE;

    private static final String AVG_DOWNSAMPLE = "SELECT AVG(%s) FROM " + PREFIX + "%s " + GROUP_BY_CLAUSE;

    private static final String COUNT_DOWNSAMPLE = "SELECT COUNT(%s) FROM " + PREFIX + "%s " + GROUP_BY_CLAUSE;

    private static final String SUM_DOWNSAMPLE = "SELECT SUM(%s) FROM " + PREFIX + "%s " + GROUP_BY_CLAUSE;

    private static final String SHOW_TIMESERIES = "SHOW TIMESERIES";

    private final Map<Long, SessionPool> sessionPools;

    public IoTDBPlanExecutor(List<StorageEngineMeta> storageEngineMetaList) {
        sessionPools = new ConcurrentHashMap<>();
        for (StorageEngineMeta storageEngineMeta : storageEngineMetaList) {
            if (!createSessionPool(storageEngineMeta)) {
                System.exit(1);
            }
        }
    }

    public static boolean testConnection(StorageEngineMeta storageEngineMeta) {
        Map<String, String> extraParams = storageEngineMeta.getExtraParams();
        String username = extraParams.getOrDefault("username", "root");
        String password = extraParams.getOrDefault("password", "root");

        Session session = new Session(storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password);

        try {
            session.open(false);
            session.close();
        } catch (IoTDBConnectionException e) {
            logger.error("test connection error: {}", e.getMessage());
            return false;
        }
        return true;
    }

    private boolean createSessionPool(StorageEngineMeta storageEngineMeta) {
        if (storageEngineMeta.getDbType() != StorageEngine.IoTDB) {
            logger.warn("unexpected database: " + storageEngineMeta.getDbType());
            return false;
        }
        if (!testConnection(storageEngineMeta)) {
            logger.error("cannot connect to " + storageEngineMeta.toString());
            return false;
        }
        Map<String, String> extraParams = storageEngineMeta.getExtraParams();
        String username = extraParams.getOrDefault("username", "root");
        String password = extraParams.getOrDefault("password", "root");
        int sessionPoolSize = Integer.parseInt(extraParams.getOrDefault("sessionPoolSize", "100"));
        SessionPool sessionPool = new SessionPool(storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password, sessionPoolSize);
        sessionPools.put(storageEngineMeta.getId(), sessionPool);
        return true;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertNonAlignedColumnRecordsPlan(InsertNonAlignedColumnRecordsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<Map<String, Tablet>> tabletsList = new ArrayList<>();
        Map<Integer, List<Integer>> tabletsIndexToPathsIndexes = new HashMap<>();

        // 创建 Tablet
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + plan.getPath(i).substring(0, plan.getPath(i).lastIndexOf('.'));
            String measurement = plan.getPath(i).substring(plan.getPath(i).lastIndexOf('.') + 1);
            int j = 0;
            for (Map<String, Tablet> tablets : tabletsList) {
                if (tablets.containsKey(deviceId)) {
                    j++;
                }
            }
            if (j == tabletsList.size()) {
                Map<String, Tablet> tablets = new HashMap<>();
                tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), BATCH_SIZE));
                tabletsList.add(tablets);
                List<Integer> pathsIndexes = new ArrayList<>();
                pathsIndexes.add(i);
                tabletsIndexToPathsIndexes.put(j, pathsIndexes);
            } else {
                tabletsList.get(j).put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), BATCH_SIZE));
                tabletsIndexToPathsIndexes.get(j).add(i);
            }
        }

        for (Map.Entry<Integer, List<Integer>> entry : tabletsIndexToPathsIndexes.entrySet()) {
            int cnt = 0;
            int[] indexes = new int[entry.getValue().size()];
            do {
                int size = Math.min(plan.getTimestamps().length - cnt, BATCH_SIZE);

                // 插入 timestamps 和 values
                for (int i = 0; i < entry.getValue().size(); i++) {
                    String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + plan.getPath(entry.getValue().get(i)).substring(0, plan.getPath(entry.getValue().get(i)).lastIndexOf('.'));
                    String measurement = plan.getPath(entry.getValue().get(i)).substring(plan.getPath(entry.getValue().get(i)).lastIndexOf('.') + 1);
                    Tablet tablet = tabletsList.get(entry.getKey()).get(deviceId);
                    for (int j = cnt; j < cnt + size; j++) {
                        if (plan.getBitmap(entry.getValue().get(i)).get(j)) {
                            int row = tablet.rowSize++;
                            tablet.addTimestamp(row, plan.getTimestamp(j));
                            if (plan.getDataType(entry.getValue().get(i)) == BINARY) {
                                tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(entry.getValue().get(i))[indexes[i]]));
                            } else {
                                tablet.addValue(measurement, row, plan.getValues(entry.getValue().get(i))[indexes[i]]);
                            }
                            indexes[i]++;
                        }
                    }
                }

                try {
                    sessionPool.insertTablets(tabletsList.get(entry.getKey()));
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    logger.error(e.getMessage());
                    return new NonDataPlanExecuteResult(FAILURE, plan);
                }

                for (Tablet tablet : tabletsList.get(entry.getKey()).values()) {
                    tablet.reset();
                }
                cnt += size;
            } while (cnt < plan.getTimestamps().length);
        }

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        Map<String, Tablet> tablets = new HashMap<>();

        // 创建 tablets
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String path = plan.getPath(i);
            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), BATCH_SIZE));
        }

        int cnt = 0;
        do {
            int size = Math.min(plan.getTimestamps().length - cnt, BATCH_SIZE);

            // 插入 timestamps 和 values
            for (int i = 0; i < plan.getPathsNum(); i++) {
                String path = plan.getPath(i);
                String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
                String measurement = path.substring(path.lastIndexOf('.') + 1);
                Tablet tablet = tablets.get(deviceId);
                for (int j = cnt; j < cnt + size; j++) {
                    int row = tablet.rowSize++;
                    tablet.addTimestamp(row, plan.getTimestamp(j));
                    if (plan.getDataType(i) == BINARY) {
                        tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(i)[j]));
                    } else {
                        tablet.addValue(measurement, row, plan.getValues(i)[j]);
                    }
                }
            }

            try {
                sessionPool.insertTablets(tablets);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
                return new NonDataPlanExecuteResult(FAILURE, plan);
            }

            for (Tablet tablet : tablets.values()) {
                tablet.reset();
            }
            cnt += size;
        } while (cnt < plan.getTimestamps().length);

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertNonAlignedRowRecordsPlan(InsertNonAlignedRowRecordsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<Map<String, Tablet>> tabletsList = new ArrayList<>();
        // 将 paths 按照 deviceId 分组
        // 例如：paths = {d1.s11, d1.s12, d1.s13, d2.s21, d2.s22, d3.s31}
        // 那么 paths 会被分为 {d1.s11, d2.s21, d3.s31} {d1.s12, d2.s22} {d1.s13} 三组
        // tabletsIndexToPathsIndexes = <0, {0, 3, 5}>, <1, {1, 4}>, <2, {2}>
        Map<Integer, List<Integer>> tabletsIndexToPathsIndexes = new HashMap<>();
        // 解析 bitmap
        // 例如：plan.getBitmapList() = {
        //   [1, 1, 1, 1, 1, 1],
        //   [1, 0, 1, 0, 1, 0],
        //   [0, 1, 1, 0, 1, 1],
        // }
        // 那么 pathsIndexToValuesIndexes = <0, [0, 0, -1]>, <1, [1, -1, 0]>, <2, [2, 1, 1]>,
        // <3, [3, -1, -1]>, <4, [4, 2, 2]>, <5, [5, -1, 3]>
        Map<Integer, int[]> pathsIndexToValuesIndexes = new HashMap<>();

        // 创建 Tablet
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + plan.getPath(i).substring(0, plan.getPath(i).lastIndexOf('.'));
            String measurement = plan.getPath(i).substring(plan.getPath(i).lastIndexOf('.') + 1);
            int j = 0;
            for (Map<String, Tablet> tablets : tabletsList) {
                if (tablets.containsKey(deviceId)) {
                    j++;
                }
            }
            if (j == tabletsList.size()) {
                Map<String, Tablet> tablets = new HashMap<>();
                tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), BATCH_SIZE));
                tabletsList.add(tablets);
                List<Integer> pathsIndexes = new ArrayList<>();
                pathsIndexes.add(i);
                tabletsIndexToPathsIndexes.put(j, pathsIndexes);
            } else {
                tabletsList.get(j).put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), BATCH_SIZE));
                tabletsIndexToPathsIndexes.get(j).add(i);
            }
        }

        // 解析 bitmap
        for (int i = 0; i < plan.getPathsNum(); i++) {
            pathsIndexToValuesIndexes.put(i, new int[plan.getTimestamps().length]);
        }
        for (int i = 0; i < plan.getTimestamps().length; i++) {
            int cnt = 0;
            for (int j = 0; j < plan.getPathsNum(); j++) {
                if (plan.getBitmap(i).get(j)) {
                    pathsIndexToValuesIndexes.get(j)[i] = cnt;
                    cnt++;
                } else {
                    pathsIndexToValuesIndexes.get(j)[i] = -1;
                }
            }
        }

        for (Map.Entry<Integer, List<Integer>> entry : tabletsIndexToPathsIndexes.entrySet()) {
            int cnt = 0;
            do {
                int size = Math.min(plan.getTimestamps().length - cnt, BATCH_SIZE);

                // 插入 timestamps 和 values
                for (int i = cnt; i < cnt + size; i++) {
                    for (Integer index : entry.getValue()) {
                        if (pathsIndexToValuesIndexes.get(index)[i] != -1) {
                            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + plan.getPath(index).substring(0, plan.getPath(index).lastIndexOf('.'));
                            String measurement = plan.getPath(index).substring(plan.getPath(index).lastIndexOf('.') + 1);
                            Tablet tablet = tabletsList.get(entry.getKey()).get(deviceId);
                            int row = tablet.rowSize++;
                            tablet.addTimestamp(row, plan.getTimestamp(i));
                            if (plan.getDataType(index) == BINARY) {
                                tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(i)[pathsIndexToValuesIndexes.get(index)[i]]));
                            } else {
                                tablet.addValue(measurement, row, plan.getValues(i)[pathsIndexToValuesIndexes.get(index)[i]]);
                            }
                        }
                    }
                }

                try {
                    sessionPool.insertTablets(tabletsList.get(entry.getKey()));
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    logger.error(e.getMessage());
                    return new NonDataPlanExecuteResult(FAILURE, plan);
                }

                for (Tablet tablet : tabletsList.get(entry.getKey()).values()) {
                    tablet.reset();
                }
                cnt += size;
            } while (cnt < plan.getTimestamps().length);
        }

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        Map<String, Tablet> tablets = new HashMap<>();

        // 创建 tablets
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String path = plan.getPath(i);
            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), BATCH_SIZE));
        }

        int cnt = 0;
        do {
            int size = Math.min(plan.getTimestamps().length - cnt, BATCH_SIZE);

            // 插入 timestamps 和 values
            for (int i = cnt; i < cnt + size; i++) {
                for (int j = 0; j < plan.getPathsNum(); j++) {
                    String path = plan.getPath(i);
                    String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
                    String measurement = path.substring(path.lastIndexOf('.') + 1);
                    Tablet tablet = tablets.get(deviceId);
                    int row = tablet.rowSize++;
                    tablet.addTimestamp(row, plan.getTimestamp(i));
                    if (plan.getDataType(j) == BINARY) {
                        tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(i)[j]));
                    } else {
                        tablet.addValue(measurement, row, plan.getValues(i)[j]);
                    }
                }
            }

            try {
                sessionPool.insertTablets(tablets);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
                return new NonDataPlanExecuteResult(FAILURE, plan);
            }

            for (Tablet tablet : tablets.values()) {
                tablet.reset();
            }
            cnt += size;
        } while (cnt < plan.getTimestamps().length);

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    public QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(QUERY_DATA, pair.v, pair.k, plan.getStartTime(), plan.getEndTime());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new QueryDataPlanExecuteResult(FAILURE, plan, null);
        }
        return new QueryDataPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            if (plan.getPaths().size() == 1 && plan.getPaths().get(0).equals("*")) {
                sessionPool.executeNonQueryStatement(String.format(DELETE_STORAGE_GROUP_CLAUSE, plan.getStorageUnit().getId()));
            } else {
                for (String path : plan.getPaths()) {
                    sessionPool.executeNonQueryStatement(String.format(DELETE_TIMESERIES_CLAUSE, plan.getStorageUnit().getId() + "." + path));
                }
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new NonDataPlanExecuteResult(FAILURE, plan);
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            // change [start, end] to [start, end)
            sessionPool.deleteData(plan.getPaths().stream().map(x -> PREFIX + plan.getStorageUnit().getId() + "." + x).collect(Collectors.toList()), plan.getStartTime(), plan.getEndTime()-1);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new NonDataPlanExecuteResult(FAILURE, plan);
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public StorageEngineChangeHook getStorageEngineChangeHook() {
        return (before, after) -> {
            if (before == null && after != null) {
                logger.info("a new iotdb engine added: " + after.getIp() + ":" + after.getPort());
                createSessionPool(after);
            }
            // TODO: 考虑结点删除等情况
        };
    }

    @Override
    public AvgAggregateQueryPlanExecuteResult syncExecuteAvgQueryPlan(AvgQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Long> counts = new ArrayList<>();
        List<Object> sums = new ArrayList<>();
        for (String path : plan.getPaths()) {
            Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
            SessionDataSetWrapper dataSet;
            RowRecord rowRecord;
            try {
                dataSet = sessionPool.executeQueryStatement(String.format(AVG, pair.v, pair.v, pair.k, plan.getStartTime(), plan.getEndTime()));
                rowRecord = dataSet.next();
                if (rowRecord != null && !rowRecord.getFields().isEmpty()) {
                    for (int i = 0; i < rowRecord.getFields().size() / 2; i++) {
                        String columnName = dataSet.getColumnNames().get(i);
                        String tempPath = columnName.substring(columnName.indexOf('(') + 1, columnName.indexOf(')'));
                        if (rowRecord.getFields().get(i) != null && rowRecord.getFields().get(rowRecord.getFields().size() / 2 + i) != null) {
                            paths.add(tempPath.substring(tempPath.indexOf('.', tempPath.indexOf('.') + 1) + 1));
                            dataTypeList.add(fromIoTDB(dataSet.getColumnTypes().get(rowRecord.getFields().size() / 2 + i)));
                            counts.add(rowRecord.getFields().get(i).getLongV());
                            if (rowRecord.getFields().get(rowRecord.getFields().size() / 2 + i).getDataType() != TSDataType.TEXT) {
                                sums.add(rowRecord.getFields().get(rowRecord.getFields().size() / 2 + i).getObjectValue(dataSet.getColumnTypes().get(rowRecord.getFields().size() / 2 + i)));
                            } else {
                                sums.add(rowRecord.getFields().get(rowRecord.getFields().size() / 2 + i).getBinaryV().getValues());
                            }
                        }
                    }
                }
                dataSet.close();
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                boolean isText = false;
                if (e.getMessage().contains("Unsupported data type in aggregation SUM : TEXT")) {
                    isText = true;
                    if (!path.contains("*")) {
                        continue;
                    }
                }
                if (isText) {
                    logger.error("Unsupported data type in aggregation SUM : TEXT");
                }
                logger.error(e.getMessage());
                return new AvgAggregateQueryPlanExecuteResult(FAILURE, null);
            }
        }
        AvgAggregateQueryPlanExecuteResult result = new AvgAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setDataTypes(dataTypeList);
        result.setCounts(counts);
        result.setSums(sums);
        return result;
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteCountQueryPlan(CountQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            List<String> paths = new ArrayList<>();
            List<DataType> dataTypeList = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(COUNT, pair.v, pair.k, plan.getStartTime(), plan.getEndTime()));
                RowRecord rowRecord = dataSet.next();
                if (rowRecord != null && !rowRecord.getFields().isEmpty()) {
                    for (int i = 0; i < rowRecord.getFields().size(); i++) {
                        if (rowRecord.getFields().get(i) != null) {
                            String columnName = dataSet.getColumnNames().get(i);
                            String tempPath = columnName.substring(columnName.indexOf('(') + 1, columnName.indexOf(')'));
                            paths.add(tempPath.substring(tempPath.indexOf('.', tempPath.indexOf('.') + 1) + 1));
                            dataTypeList.add(fromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (dataSet.getColumnTypes().get(i) != TSDataType.TEXT) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(dataSet.getColumnTypes().get(i)));
                            } else {
                                values.add(rowRecord.getFields().get(i).getBinaryV().getValues());
                            }
                        }
                    }
                }
                dataSet.close();
            }
            StatisticsAggregateQueryPlanExecuteResult result = new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(paths);
            result.setDataTypes(dataTypeList);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new StatisticsAggregateQueryPlanExecuteResult(FAILURE, plan);
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteSumQueryPlan(SumQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<String> paths = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        List<Object> values = new ArrayList<>();
        for (String path : plan.getPaths()) {
            Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
            SessionDataSetWrapper dataSet;
            RowRecord rowRecord;
            try {
                dataSet = sessionPool.executeQueryStatement(String.format(SUM, pair.v, pair.k, plan.getStartTime(), plan.getEndTime()));
                rowRecord = dataSet.next();
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                boolean isText = false;
                if (e.getMessage().contains("Unsupported data type in aggregation SUM : TEXT")) {
                    isText = true;
                    if (!path.contains("*")) {
                        continue;
                    }
                }
                if (isText) {
                    logger.error("Unsupported data type in aggregation SUM : TEXT");
                }
                logger.error(e.getMessage());
                return new StatisticsAggregateQueryPlanExecuteResult(FAILURE, null);
            }
            if (rowRecord != null && !rowRecord.getFields().isEmpty()) {
                for (int i = 0; i < rowRecord.getFields().size(); i++) {
                    if (rowRecord.getFields().get(i) != null) {
                        String columnName = dataSet.getColumnNames().get(i);
                        String tempPath = columnName.substring(columnName.indexOf('(') + 1, columnName.indexOf(')'));
                        paths.add(tempPath.substring(tempPath.indexOf('.', tempPath.indexOf('.') + 1) + 1));
                        dataTypeList.add(fromIoTDB(dataSet.getColumnTypes().get(i)));
                        if (rowRecord.getFields().get(i).getDataType() != TSDataType.TEXT) {
                            values.add(rowRecord.getFields().get(i).getObjectValue(dataSet.getColumnTypes().get(i)));
                        } else {
                            values.add(rowRecord.getFields().get(i).getBinaryV().getValues());
                        }
                    }
                }
            }
            dataSet.close();
        }
        StatisticsAggregateQueryPlanExecuteResult result = new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
        result.setPaths(paths);
        result.setDataTypes(dataTypeList);
        result.setValues(values);
        return result;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstQueryPlan(FirstQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            List<String> paths = new ArrayList<>();
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(FIRST_VALUE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime()));
                RowRecord rowRecord = dataSet.next();
                if (rowRecord != null && !rowRecord.getFields().isEmpty()) {
                    for (int i = 0; i < rowRecord.getFields().size(); i++) {
                        if (rowRecord.getFields().get(i) != null && !rowRecord.getFields().get(i).getStringValue().equals("null")) {
                            timestamps.add(plan.getStartTime());
                            String columnName = dataSet.getColumnNames().get(i);
                            String tempPath = columnName.substring(columnName.indexOf('(') + 1, columnName.indexOf(')'));
                            paths.add(tempPath.substring(tempPath.indexOf('.', tempPath.indexOf('.') + 1) + 1));
                            dataTypeList.add(fromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (dataSet.getColumnTypes().get(i) != TSDataType.TEXT) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(dataSet.getColumnTypes().get(i)));
                            } else {
                                values.add(rowRecord.getFields().get(i).getBinaryV().getValues());
                            }
                        }
                    }
                }
                dataSet.close();
            }
            SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(paths);
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, plan);
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            List<String> paths = new ArrayList<>();
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(LAST_VALUE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime()));
                RowRecord rowRecord = dataSet.next();
                if (rowRecord != null && !rowRecord.getFields().isEmpty()) {
                    for (int i = 0; i < rowRecord.getFields().size(); i++) {
                        if (rowRecord.getFields().get(i) != null && !rowRecord.getFields().get(i).getStringValue().equals("null")) {
                            timestamps.add(plan.getStartTime());
                            String columnName = dataSet.getColumnNames().get(i);
                            String tempPath = columnName.substring(columnName.indexOf('(') + 1, columnName.indexOf(')'));
                            paths.add(tempPath.substring(tempPath.indexOf('.', tempPath.indexOf('.') + 1) + 1));
                            dataTypeList.add(fromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (dataSet.getColumnTypes().get(i) != TSDataType.TEXT) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(dataSet.getColumnTypes().get(i)));
                            } else {
                                values.add(rowRecord.getFields().get(i).getBinaryV().getValues());
                            }
                        }
                    }
                }
                dataSet.close();
            }
            SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(paths);
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, plan);
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMaxQueryPlan(MaxQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            List<String> paths = new ArrayList<>();
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(MAX_VALUE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime()));
                RowRecord rowRecord = dataSet.next();
                if (rowRecord != null && !rowRecord.getFields().isEmpty()) {
                    for (int i = 0; i < rowRecord.getFields().size(); i++) {
                        if (rowRecord.getFields().get(i) != null && !rowRecord.getFields().get(i).getStringValue().equals("null")) {
                            timestamps.add(-1L);
                            String columnName = dataSet.getColumnNames().get(i);
                            String tempPath = columnName.substring(columnName.indexOf('(') + 1, columnName.indexOf(')'));
                            paths.add(tempPath.substring(tempPath.indexOf('.', tempPath.indexOf('.') + 1) + 1));
                            dataTypeList.add(fromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (dataSet.getColumnTypes().get(i) != TSDataType.TEXT) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(dataSet.getColumnTypes().get(i)));
                            } else {
                                values.add(rowRecord.getFields().get(i).getBinaryV().getValues());
                            }
                        }
                    }
                }
                dataSet.close();
            }
            SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(paths);
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, plan);
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMinQueryPlan(MinQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            List<String> paths = new ArrayList<>();
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(MIN_VALUE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime()));
                RowRecord rowRecord = dataSet.next();
                if (rowRecord != null && !rowRecord.getFields().isEmpty()) {
                    for (int i = 0; i < rowRecord.getFields().size(); i++) {
                        if (rowRecord.getFields().get(i) != null && !rowRecord.getFields().get(i).getStringValue().equals("null")) {
                            String columnName = dataSet.getColumnNames().get(i);
                            String tempPath = columnName.substring(columnName.indexOf('(') + 1, columnName.indexOf(')'));
                            timestamps.add(-1L);
                            paths.add(tempPath.substring(tempPath.indexOf('.', tempPath.indexOf('.') + 1) + 1));
                            dataTypeList.add(fromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (dataSet.getColumnTypes().get(i) != TSDataType.TEXT) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(dataSet.getColumnTypes().get(i)));
                            } else {
                                values.add(rowRecord.getFields().get(i).getBinaryV().getValues());
                            }
                        }
                    }
                }
                dataSet.close();
            }
            SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(paths);
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, plan);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleAvgQueryPlan(DownsampleAvgQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(AVG_DOWNSAMPLE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new DownsampleQueryPlanExecuteResult(FAILURE, plan, null);
        }
        return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleCountQueryPlan(DownsampleCountQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(COUNT_DOWNSAMPLE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new DownsampleQueryPlanExecuteResult(FAILURE, plan, null);
        }
        return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleSumQueryPlan(DownsampleSumQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(SUM_DOWNSAMPLE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new DownsampleQueryPlanExecuteResult(FAILURE, plan, null);
        }
        return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMaxQueryPlan(DownsampleMaxQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(MAX_VALUE_DOWNSAMPLE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new DownsampleQueryPlanExecuteResult(FAILURE, plan, null);
        }
        return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMinQueryPlan(DownsampleMinQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(MIN_VALUE_DOWNSAMPLE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new DownsampleQueryPlanExecuteResult(FAILURE, plan, null);
        }
        return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstQueryPlan(DownsampleFirstQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(FIRST_VALUE_DOWNSAMPLE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new DownsampleQueryPlanExecuteResult(FAILURE, plan, null);
        }
        return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastQueryPlan(DownsampleLastQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(LAST_VALUE_DOWNSAMPLE, pair.v, pair.k, plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new DownsampleQueryPlanExecuteResult(FAILURE, plan, null);
        }
        return new DownsampleQueryPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }


    @Override
    public ValueFilterQueryPlanExecuteResult syncExecuteValueFilterQueryPlan(ValueFilterQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(QUERY_DATA, pair.v, pair.k, plan.getStartTime(), plan.getEndTime());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new ValueFilterQueryPlanExecuteResult(FAILURE, plan, null);
        }
        return new ValueFilterQueryPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public ShowColumnsPlanExecuteResult syncExecuteShowColumnsPlan(ShowColumnsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<String> paths = new ArrayList<>();
        List<DataType> dataTypes = new ArrayList<>();
        try {
            SessionDataSetWrapper dataSet = sessionPool.executeQueryStatement(SHOW_TIMESERIES);
            while (dataSet.hasNext()) {
                RowRecord record = dataSet.next();
                if (record == null || record.getFields().size() < 4) {
                    continue;
                }
                String path = record.getFields().get(0).getStringValue();
                path = path.substring(5);
                path = path.substring(path.indexOf('.') + 1);
                String dataTypeName = record.getFields().get(3).getStringValue();
                switch (dataTypeName) {
                    case "BOOLEAN":
                        paths.add(path);
                        dataTypes.add(DataType.BOOLEAN);
                        break;
                    case "FLOAT":
                        paths.add(path);
                        dataTypes.add(DataType.FLOAT);
                        break;
                    case "TEXT":
                        paths.add(path);
                        dataTypes.add(BINARY);
                        break;
                    case "DOUBLE":
                        paths.add(path);
                        dataTypes.add(DOUBLE);
                        break;
                    case "INT32":
                        paths.add(path);
                        dataTypes.add(INTEGER);
                        break;
                    case "INT64":
                        paths.add(path);
                        dataTypes.add(LONG);
                        break;
                }
            }
            dataSet.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new ShowColumnsPlanExecuteResult(FAILURE, plan);
        }
        return new ShowColumnsPlanExecuteResult(SUCCESS, plan, paths, dataTypes);
    }

    private Pair<String, String> generateDeviceAndMeasurement(String path, String storageUnitId) {
        String deviceId = storageUnitId;
        String measurement;
        if (path.equals("*")) {
            measurement = "*";
        } else {
            if (!path.contains(".")) {
                measurement = path;
            } else {
                deviceId += "." + path.substring(0, path.lastIndexOf('.'));
                measurement = path.substring(path.lastIndexOf('.') + 1);
            }
        }
        return new Pair<>(deviceId, measurement);
    }
}
