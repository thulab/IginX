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

import cn.edu.tsinghua.iginx.iotdb.query.entity.IoTDBQueryExecuteDataSet;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.LastValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.ShowColumnsPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.ValueFilterQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.query.IStorageEngine;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DownsampleQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.LastQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ShowColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ValueFilterQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.commons.lang3.RandomStringUtils;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.*;
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

    private static final String LAST = "SELECT LAST %s FROM " + PREFIX + "%s WHERE time >= %d";

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

    private static final String SHOW_STORAGE_GROUP = "SHOW STORAGE GROUP";

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
        if (!storageEngineMeta.getStorageEngine().equals("iotdb12")) {
            logger.warn("unexpected database: " + storageEngineMeta.getStorageEngine());
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
    public NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
        boolean isMaster = false;
        if (plan.isSync()) {
            isMaster = true;
        }
        // TODO 每个 tablet 内部都是对齐的，不同 tablet 之间可以不对齐
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        Map<String, Tablet> tablets = new HashMap<>();
        Map<String, List<MeasurementSchema>> schemasMap = new HashMap<>();
        Map<String, List<Integer>> deviceIdToPathIndexes = new HashMap<>();
        int batchSize = Math.min(plan.getTimestamps().length, BATCH_SIZE);

        // 创建 tablets
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String path = plan.getPath(i);
            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            List<MeasurementSchema> schemaList;
            List<Integer> pathIndexes;
            if (schemasMap.containsKey(deviceId)) {
                schemaList = schemasMap.get(deviceId);
                pathIndexes = deviceIdToPathIndexes.get(deviceId);
            } else {
                schemaList = new ArrayList<>();
                pathIndexes = new ArrayList<>();
            }
            schemaList.add(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i))));
            schemasMap.put(deviceId, schemaList);
            pathIndexes.add(i);
            deviceIdToPathIndexes.put(deviceId, pathIndexes);
        }

        for (Map.Entry<String, List<MeasurementSchema>> entry : schemasMap.entrySet()) {
            tablets.put(entry.getKey(), new Tablet(entry.getKey(), entry.getValue(), batchSize));
        }
        String sign = RandomStringUtils.randomAlphanumeric(3);
        long start = System.currentTimeMillis();
        logger.info("Start Iotdb Insert, sign : {}, timestamp : {}", sign, start);

        int cnt = 0;
        int[] indexes = new int[plan.getPathsNum()];
        do {
            int size = Math.min(plan.getTimestamps().length - cnt, batchSize);

            // 插入 timestamps 和 values
            for (Map.Entry<String, List<Integer>> entry : deviceIdToPathIndexes.entrySet()) {
                String deviceId = entry.getKey();
                Tablet tablet = tablets.get(deviceId);
                for (int i = cnt; i < cnt + size; i++) {
                    if (plan.getBitmap(entry.getValue().get(0)).get(i)) {
                        int row = tablet.rowSize++;
                        tablet.addTimestamp(row, plan.getTimestamp(i));
                        for (Integer j : entry.getValue()) {
                            String path = plan.getPath(j);
                            String measurement = path.substring(path.lastIndexOf('.') + 1);
                            if (plan.getDataType(j) == BINARY) {
                                tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(j)[indexes[j]]));
                            } else {
                                tablet.addValue(measurement, row, plan.getValues(j)[indexes[j]]);
                            }
                            indexes[j]++;
                        }
                    }
                }
            }

            try {
                logger.info("Before insertTablets, sign : {}, cost : {}", sign, System.currentTimeMillis() - start);
                sessionPool.insertTablets(tablets);
                logger.info("After insertTablets, sign : {}, cost : {}", sign, System.currentTimeMillis() - start);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
                return new NonDataPlanExecuteResult(FAILURE, plan);
            }

            for (Tablet tablet : tablets.values()) {
                tablet.reset();
            }
            cnt += size;
        } while(cnt < plan.getTimestamps().length);
        logger.info("End Iotdb Insert, sign : {}, cos : {}, isMaster: {}, size: {}, engine: {}", sign, System.currentTimeMillis() - start, isMaster, plan.getPaths().size(), plan.getStorageEngineId());

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertNonAlignedColumnRecordsPlan(InsertNonAlignedColumnRecordsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        Map<Integer, Map<String, Tablet>> tabletsMap = new HashMap<>();
        Map<Integer, List<Integer>> tabletIndexToPathIndexes = new HashMap<>();
        Map<String, Integer> deviceIdToCnt = new HashMap<>();
        int batchSize = Math.min(plan.getTimestamps().length, BATCH_SIZE);

        // 创建 tablets
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String path = plan.getPath(i);
            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            int measurementNum;
            List<Integer> pathIndexes;
            Map<String, Tablet> tablets;

            measurementNum = deviceIdToCnt.computeIfAbsent(deviceId, x -> -1);
            deviceIdToCnt.put(deviceId, measurementNum + 1);
            pathIndexes = tabletIndexToPathIndexes.computeIfAbsent(measurementNum + 1, x -> new ArrayList<>());
            pathIndexes.add(i);
            tabletIndexToPathIndexes.put(measurementNum + 1, pathIndexes);
            tablets = tabletsMap.computeIfAbsent(measurementNum + 1, x -> new HashMap<>());
            tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), batchSize));
            tabletsMap.put(measurementNum + 1, tablets);
        }

        for (Map.Entry<Integer, List<Integer>> entry : tabletIndexToPathIndexes.entrySet()) {
            int cnt = 0;
            int[] indexesOfBitmap = new int[entry.getValue().size()];
            do {
                int size = Math.min(plan.getTimestamps().length - cnt, batchSize);

                // 插入 timestamps 和 values
                for (int i = 0; i < entry.getValue().size(); i++) {
                    int index = entry.getValue().get(i);
                    String path = plan.getPath(index);
                    String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
                    String measurement = path.substring(path.lastIndexOf('.') + 1);
                    Tablet tablet = tabletsMap.get(entry.getKey()).get(deviceId);
                    for (int j = cnt; j < cnt + size; j++) {
                        if (plan.getBitmap(index).get(j)) {
                            int row = tablet.rowSize++;
                            tablet.addTimestamp(row, plan.getTimestamp(j));
                            if (plan.getDataType(index) == BINARY) {
                                tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(index)[indexesOfBitmap[i]]));
                            } else {
                                tablet.addValue(measurement, row, plan.getValues(index)[indexesOfBitmap[i]]);
                            }
                            indexesOfBitmap[i]++;
                        }
                    }
                }

                try {
                    sessionPool.insertTablets(tabletsMap.get(entry.getKey()));
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    logger.error(e.getMessage());
                    return new NonDataPlanExecuteResult(FAILURE, plan);
                }

                for (Tablet tablet : tabletsMap.get(entry.getKey()).values()) {
                    tablet.reset();
                }
                cnt += size;
            } while(cnt < plan.getTimestamps().length);
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan) {
        boolean isMaster = false;
        if (plan.isSync()) {
            isMaster = true;
        }
        // TODO 每个 tablet 内部都是对齐的，不同 tablet 之间可以不对齐
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        Map<String, Tablet> tablets = new HashMap<>();
        Map<String, List<MeasurementSchema>> schemasMap = new HashMap<>();
        Map<String, List<Integer>> deviceIdToPathIndexes = new HashMap<>();
        int batchSize = Math.min(plan.getTimestamps().length, BATCH_SIZE);

        // 创建 tablets
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String path = plan.getPath(i);
            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            List<MeasurementSchema> schemaList;
            List<Integer> pathIndexes;
            if (schemasMap.containsKey(deviceId)) {
                schemaList = schemasMap.get(deviceId);
                pathIndexes = deviceIdToPathIndexes.get(deviceId);
            } else {
                schemaList = new ArrayList<>();
                pathIndexes = new ArrayList<>();
            }

            schemaList.add(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i))));
            schemasMap.put(deviceId, schemaList);
            pathIndexes.add(i);
            deviceIdToPathIndexes.put(deviceId, pathIndexes);
        }

        for (Map.Entry<String, List<MeasurementSchema>> entry : schemasMap.entrySet()) {
            tablets.put(entry.getKey(), new Tablet(entry.getKey(), entry.getValue(), batchSize));
        }
        String sign = RandomStringUtils.randomAlphanumeric(3);
        long start = System.currentTimeMillis();
        logger.info("Start Iotdb Insert, sign : {}, timestamp : {}", sign, start);
        int cnt = 0;
        do {
            int size = Math.min(plan.getTimestamps().length - cnt, batchSize);
            // 对于每个时间戳，需要记录每个 deviceId 对应的 tablet 的 row 的变化
            Map<String, Integer> deviceIdToRow = new HashMap<>();

            // 插入 timestamps 和 values
            for (int i = cnt; i < cnt + size; i++) {
                int index = 0;
                deviceIdToRow.clear();
                for (int j = 0; j < plan.getPathsNum(); j++) {
                    if (plan.getBitmap(i).get(j)) {
                        String path = plan.getPath(j);
                        String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
                        String measurement = path.substring(path.lastIndexOf('.') + 1);
                        Tablet tablet = tablets.get(deviceId);
                        if (!deviceIdToRow.containsKey(deviceId)) {
                            int row = tablet.rowSize++;
                            tablet.addTimestamp(row, plan.getTimestamp(i));
                            deviceIdToRow.put(deviceId, row);
                        }
                        if (plan.getDataType(j) == BINARY) {
                            tablet.addValue(measurement, deviceIdToRow.get(deviceId), new Binary((byte[]) plan.getValues(i)[index]));
                        } else {
                            tablet.addValue(measurement, deviceIdToRow.get(deviceId), plan.getValues(i)[index]);
                        }
                        index++;
                    }
                }
            }

            try {
                logger.info("Before insertTablets, sign : {}, cost : {}", sign, System.currentTimeMillis() - start);
                sessionPool.insertTablets(tablets);
                logger.info("After insertTablets, sign : {}, cost : {}", sign, System.currentTimeMillis() - start);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
                return new NonDataPlanExecuteResult(FAILURE, plan);
            }

            for (Tablet tablet : tablets.values()) {
                tablet.reset();
            }
            cnt += size;
        } while(cnt < plan.getTimestamps().length);

        logger.info("End Iotdb Insert, sign : {}, cos : {}, isMaster: {}, size: {}, engine: {}", sign, System.currentTimeMillis() - start, isMaster, plan.getPaths().size(), plan.getStorageEngineId());
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertNonAlignedRowRecordsPlan(InsertNonAlignedRowRecordsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        Map<Integer, Map<String, Tablet>> tabletsMap = new HashMap<>();
        Map<Integer, Integer> pathIndexToTabletIndex = new HashMap<>();
        Map<String, Integer> deviceIdToCnt = new HashMap<>();
        int batchSize = Math.min(plan.getTimestamps().length, BATCH_SIZE);

        // 创建 tablets
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String path = plan.getPath(i);
            String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            int measurementNum;
            Map<String, Tablet> tablets;

            measurementNum = deviceIdToCnt.computeIfAbsent(deviceId, x -> -1);
            deviceIdToCnt.put(deviceId, measurementNum + 1);
            pathIndexToTabletIndex.put(i, measurementNum + 1);
            tablets = tabletsMap.computeIfAbsent(measurementNum + 1, x -> new HashMap<>());
            tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), batchSize));
            tabletsMap.put(measurementNum + 1, tablets);
        }

        int cnt = 0;
        do {
            int size = Math.min(plan.getTimestamps().length - cnt, batchSize);
            boolean[] needToInsert = new boolean[tabletsMap.size()];
            Arrays.fill(needToInsert, false);

            // 插入 timestamps 和 values
            for (int i = cnt; i < cnt + size; i++) {
                int index = 0;
                for (int j = 0; j < plan.getPathsNum(); j++) {
                    if (plan.getBitmap(i).get(j)) {
                        String path = plan.getPath(j);
                        String deviceId = PREFIX + plan.getStorageUnit().getId() + "." + path.substring(0, path.lastIndexOf('.'));
                        String measurement = path.substring(path.lastIndexOf('.') + 1);
                        Tablet tablet = tabletsMap.get(pathIndexToTabletIndex.get(j)).get(deviceId);
                        int row = tablet.rowSize++;
                        tablet.addTimestamp(row, plan.getTimestamp(i));
                        if (plan.getDataType(j) == BINARY) {
                            tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(i)[index]));
                        } else {
                            tablet.addValue(measurement, row, plan.getValues(i)[index]);
                        }
                        needToInsert[pathIndexToTabletIndex.get(j)] = true;
                        index++;
                    }
                }
            }

            // 插入 tablets
            try {
                for (int i = 0; i < needToInsert.length; i++) {
                    if (needToInsert[i]) {
                        sessionPool.insertTablets(tabletsMap.get(i));
                    }
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
                return new NonDataPlanExecuteResult(FAILURE, plan);
            }

            // 重置 tablets
            for (int i = 0; i < needToInsert.length; i++) {
                if (needToInsert[i]) {
                    for (Tablet tablet : tabletsMap.get(i).values()) {
                        tablet.reset();
                    }
                    needToInsert[i] = false;
                }
            }
            cnt += size;
        } while(cnt < plan.getTimestamps().length);

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    public QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        boolean isMaster = false;
        if (plan.isSync()) {
            isMaster = true;
        }
        String sign = RandomStringUtils.randomAlphanumeric(3);
        long start = System.currentTimeMillis();
        logger.info("Start Iotdb Query, sign : {}, timestamp : {}", sign, start);


        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                String statement = String.format(QUERY_DATA, pair.v, pair.k, plan.getStartTime(), plan.getEndTime());
                logger.info("Before executeQueryStatement, sign : {}, cost : {}, statement: {}", sign, System.currentTimeMillis() - start, statement);
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
                logger.info("After executeQueryStatement, sign : {}, cost : {}", sign, System.currentTimeMillis() - start);
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new QueryDataPlanExecuteResult(FAILURE, plan, null);
        }
        logger.info("End Iotdb query, sign : {}, cos : {}, isMaster: {}, size: {}, engine: {}", sign, System.currentTimeMillis() - start, isMaster, plan.getPaths().size(), plan.getStorageEngineId());
        return new QueryDataPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        if (plan.getPaths().size() == 1 && plan.getPaths().get(0).equals("*")) { // 删除存储组
            try (SessionDataSetWrapper dataSet = sessionPool.executeQueryStatement(SHOW_STORAGE_GROUP)) {
                while(dataSet.hasNext()) {
                    RowRecord record = dataSet.next();
                    if (record == null) {
                        continue;
                    }
                    String storageUnitId = record.getFields().get(0).getStringValue().substring(PREFIX.length());
                    if (storageUnitId.equals(plan.getStorageUnit().getId())) {
                        sessionPool.executeNonQueryStatement(String.format(DELETE_STORAGE_GROUP_CLAUSE, plan.getStorageUnit().getId()));
                        logger.info("delete storage unit: " + storageUnitId);
                        break;
                    }
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
                return new NonDataPlanExecuteResult(FAILURE, plan);
            }
        } else { // 删除序列
            try {
                for (String path : plan.getPaths()) {
                    try {
                        sessionPool.executeNonQueryStatement(String.format(DELETE_TIMESERIES_CLAUSE, plan.getStorageUnit().getId() + "." + path));
                    } catch (StatementExecutionException e) {
                        if (e.getMessage().endsWith("does not exist;")) { // IoTDB 0.12 版本删除不存在的时间序列也会报错
                            continue;
                        }
                        logger.error(e.getMessage());
                    }
                }
            } catch (IoTDBConnectionException e) {
                logger.error(e.getMessage());
                return new NonDataPlanExecuteResult(FAILURE, plan);
            }
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            // change [start, end] to [start, end)
            sessionPool.deleteData(plan.getPaths().stream().map(x -> PREFIX + plan.getStorageUnit().getId() + "." + x).collect(Collectors.toList()), plan.getStartTime(), plan.getEndTime() - 1);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new NonDataPlanExecuteResult(FAILURE, plan);
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public LastQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan) {
        SessionPool sessionPool = sessionPools.get(plan.getStorageEngineId());
        try {
            List<String> paths = new ArrayList<>();
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                Pair<String, String> pair = generateDeviceAndMeasurement(path, plan.getStorageUnit().getId());
                SessionDataSetWrapper dataSet =
                    sessionPool.executeQueryStatement(String.format(LAST, pair.v, pair.k, plan.getStartTime()));
                while(true) {
                    RowRecord rowRecord = dataSet.next();
                    if (rowRecord == null || rowRecord.getFields().isEmpty()) {
                        break;
                    }
                    if (rowRecord.getFields().size() != 3) {
                        break;
                    }
                    if (rowRecord.getFields().get(1) != null && !rowRecord.getFields().get(1).getStringValue().equals("null")) {
                        timestamps.add(rowRecord.getTimestamp());
                        String columnName = rowRecord.getFields().get(0).getStringValue();
                        paths.add(columnName.substring(columnName.indexOf('.', columnName.indexOf('.') + 1) + 1));
                        dataTypeList.add(strFromIoTDB(dataSet.getColumnTypes().get(2)));
                        if (!dataSet.getColumnTypes().get(2).equals(TEXT)) {
                            values.add(rowRecord.getFields().get(1).getObjectValue(strToIoTDB(dataSet.getColumnTypes().get(2))));
                        } else {
                            values.add(rowRecord.getFields().get(1).getBinaryV().getValues());
                        }
                    }
                }
                dataSet.close();
            }
            LastQueryPlanExecuteResult result = new LastQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(paths);
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new LastQueryPlanExecuteResult(FAILURE, plan);
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
                            dataTypeList.add(strFromIoTDB(dataSet.getColumnTypes().get(rowRecord.getFields().size() / 2 + i)));
                            counts.add(rowRecord.getFields().get(i).getLongV());
                            if (rowRecord.getFields().get(rowRecord.getFields().size() / 2 + i).getDataType() != TSDataType.TEXT) {
                                sums.add(rowRecord.getFields().get(rowRecord.getFields().size() / 2 + i).getObjectValue(strToIoTDB(dataSet.getColumnTypes().get(rowRecord.getFields().size() / 2 + i))));
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
                return new AvgAggregateQueryPlanExecuteResult(FAILURE, plan);
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
                            dataTypeList.add(strFromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (!dataSet.getColumnTypes().get(i).equals(TEXT)) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(strToIoTDB(dataSet.getColumnTypes().get(i))));
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
                return new StatisticsAggregateQueryPlanExecuteResult(FAILURE, plan);
            }
            if (rowRecord != null && !rowRecord.getFields().isEmpty()) {
                for (int i = 0; i < rowRecord.getFields().size(); i++) {
                    if (rowRecord.getFields().get(i) != null) {
                        String columnName = dataSet.getColumnNames().get(i);
                        String tempPath = columnName.substring(columnName.indexOf('(') + 1, columnName.indexOf(')'));
                        paths.add(tempPath.substring(tempPath.indexOf('.', tempPath.indexOf('.') + 1) + 1));
                        dataTypeList.add(strFromIoTDB(dataSet.getColumnTypes().get(i)));
                        if (rowRecord.getFields().get(i).getDataType() != TSDataType.TEXT) {
                            values.add(rowRecord.getFields().get(i).getObjectValue(strToIoTDB(dataSet.getColumnTypes().get(i))));
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
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstValueQueryPlan(FirstValueQueryPlan plan) {
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
                            dataTypeList.add(strFromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (!dataSet.getColumnTypes().get(i).equals(TEXT)) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(strToIoTDB(dataSet.getColumnTypes().get(i))));
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
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteLastValueQueryPlan(LastValueQueryPlan plan) {
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
                            dataTypeList.add(strFromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (!dataSet.getColumnTypes().get(i).equals(TEXT)) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(strToIoTDB(dataSet.getColumnTypes().get(i))));
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
                            dataTypeList.add(strFromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (!dataSet.getColumnTypes().get(i).equals(TEXT)) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(strToIoTDB(dataSet.getColumnTypes().get(i))));
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
                            dataTypeList.add(strFromIoTDB(dataSet.getColumnTypes().get(i)));
                            if (!dataSet.getColumnTypes().get(i).equals(TEXT)) {
                                values.add(rowRecord.getFields().get(i).getObjectValue(strToIoTDB(dataSet.getColumnTypes().get(i))));
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
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstValueQueryPlan(DownsampleFirstValueQueryPlan plan) {
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
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastValueQueryPlan(DownsampleLastValueQueryPlan plan) {
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
            while(dataSet.hasNext()) {
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