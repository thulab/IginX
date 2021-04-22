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

import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.iotdb.query.entity.IoTDBQueryExecuteDataSet;
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
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
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

public class IoTDBPlanExecutor implements IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBPlanExecutor.class);

    private static final int BATCH_SIZE = 10000;

    private static final String PREFIX = "root.";

    private static final String TIME_RANGE_WHERE_CLAUSE = "WHERE time >= %d and time <= %d";

    private static final String QUERY_DATA = "SELECT %s FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String MAX_VALUE = "SELECT MAX_VALUE(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String MIN_VALUE = "SELECT MIN_VALUE(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String FIRST_VALUE = "SELECT FIRST_VALUE(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String LAST_VALUE = "SELECT LAST_VALUE(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String AVG = "SELECT COUNT(%s), SUM(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String COUNT = "SELECT COUNT(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    private static final String SUM = "SELECT SUM(%s) FROM " + PREFIX + "%s " + TIME_RANGE_WHERE_CLAUSE;

    // TODO 升级到0.11.3后删除
    private final Map<Long, StorageEngineMeta> storageEngineMetas;

    private final Map<Long, SessionPool> readSessionPools;

    private final Map<Long, SessionPool> writeSessionPools;

    private void createSessionPool(StorageEngineMeta storageEngineMeta) {
        if (storageEngineMeta.getDbType() != StorageEngine.IoTDB) {
            logger.warn("unexpected database: " + storageEngineMeta.getDbType());
            return;
        }
        Map<String, String> extraParams = storageEngineMeta.getExtraParams();
        String username = extraParams.getOrDefault("username", "root");
        String password = extraParams.getOrDefault("password", "root");
        int readSessions = Integer.parseInt(extraParams.getOrDefault("readSessions", "100"));
        int writeSessions = Integer.parseInt(extraParams.getOrDefault("writeSessions", "100"));
        SessionPool readSessionPool = new SessionPool(storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password, readSessions);
        SessionPool writeSessionPool = new SessionPool(storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password, writeSessions);
        readSessionPools.put(storageEngineMeta.getId(), readSessionPool);
        writeSessionPools.put(storageEngineMeta.getId(), writeSessionPool);
        storageEngineMetas.put(storageEngineMeta.getId(), storageEngineMeta);
    }

    public IoTDBPlanExecutor(List<StorageEngineMeta> storageEngineMetaList) {
        readSessionPools = new ConcurrentHashMap<>();
        writeSessionPools = new ConcurrentHashMap<>();
        storageEngineMetas = new ConcurrentHashMap<>();
        for (StorageEngineMeta storageEngineMeta: storageEngineMetaList) {
            createSessionPool(storageEngineMeta);
        }
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
        // TODO 目前 IoTDB 的 insertTablets 不支持空值，因此要求 plan 的 path 属于不同 device
        logger.info("write " + plan.getPaths().size() * plan.getTimestamps().length + " points to storage engine: " + plan.getStorageEngineId() + ".");
        SessionPool sessionPool = writeSessionPools.get(plan.getStorageEngineId());

        Map<String, Tablet> tablets = new HashMap<>();

        // 创建 Tablet
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String deviceId = PREFIX + plan.getPath(i).substring(0, plan.getPath(i).lastIndexOf('.'));
            String measurement = plan.getPath(i).substring(plan.getPath(i).lastIndexOf('.') + 1);
            tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), BATCH_SIZE));
        }

        int cnt = 0;
        int[] indexes = new int[plan.getPathsNum()];
        do {
            int size = Math.min(plan.getTimestamps().length - cnt, BATCH_SIZE);

            // 插入 timestamps 和 values
            for (int i = 0; i < plan.getPathsNum(); i++) {
                String deviceId = PREFIX + plan.getPath(i).substring(0, plan.getPath(i).lastIndexOf('.'));
                String measurement = plan.getPath(i).substring(plan.getPath(i).lastIndexOf('.') + 1);
                Tablet tablet = tablets.get(deviceId);
                for (int j = cnt; j < cnt + size; j++) {
                    if (plan.getBitmap(i).get(j)) {
                        int row = tablet.rowSize++;
                        tablet.addTimestamp(row, plan.getTimestamp(j));
                        if (plan.getDataType(i) == BINARY) {
                            tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(i)[indexes[i]]));
                        } else {
                            tablet.addValue(measurement, row, plan.getValues(i)[indexes[i]]);
                        }
                        indexes[i]++;
                    }
                }
            }

            try {
                sessionPool.insertTablets(tablets);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
            }

            for (Tablet tablet : tablets.values()) {
                tablet.reset();
            }
            cnt += size;
        } while (cnt < plan.getTimestamps().length);

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan) {
        // TODO 目前 IoTDB 的 insertTablets 不支持空值，因此要求 plan 的 path 属于不同 device
        SessionPool sessionPool = writeSessionPools.get(plan.getStorageEngineId());

        Map<String, Tablet> tablets = new HashMap<>();

        // 创建 Tablet
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String deviceId = PREFIX + plan.getPath(i).substring(0, plan.getPath(i).lastIndexOf('.'));
            String measurement = plan.getPath(i).substring(plan.getPath(i).lastIndexOf('.') + 1);
            tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(plan.getDataType(i)))), BATCH_SIZE));
        }

        int cnt = 0;
        do {
            int size = Math.min(plan.getTimestamps().length - cnt, BATCH_SIZE);

            // 插入 timestamps 和 values
            for (int i = cnt; i < cnt + size; i++) {
                int k = 0;
                for (int j = 0; j < plan.getPathsNum(); j++) {
                    if (plan.getBitmap(i).get(j)) {
                        String deviceId = PREFIX + plan.getPath(j).substring(0, plan.getPath(j).lastIndexOf('.'));
                        String measurement = plan.getPath(j).substring(plan.getPath(j).lastIndexOf('.') + 1);
                        Tablet tablet = tablets.get(deviceId);
                        int row = tablet.rowSize++;
                        tablet.addTimestamp(row, plan.getTimestamp(i));
                        if (plan.getDataType(j) == BINARY) {
                            tablet.addValue(measurement, row, new Binary((byte[]) plan.getValues(i)[k]));
                        } else {
                            tablet.addValue(measurement, row, plan.getValues(i)[k]);
                        }
                        k++;
                    }
                }
            }

            try {
                sessionPool.insertTablets(tablets);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
            }

            for (Tablet tablet : tablets.values()) {
                tablet.reset();
            }
            cnt += size;
        } while (cnt < plan.getTimestamps().length);

        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    public QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        List<QueryExecuteDataSet> sessionDataSets = new ArrayList<>();
        try {
            for (String path : plan.getPaths()) {
                String statement = String.format(QUERY_DATA, path.substring(path.lastIndexOf(".") + 1), path.substring(0, path.lastIndexOf(".")), plan.getStartTime(), plan.getEndTime());
                sessionDataSets.add(new IoTDBQueryExecuteDataSet(sessionPool.executeQueryStatement(statement)));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new QueryDataPlanExecuteResult(SUCCESS, plan, sessionDataSets);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        for (int i = 0; i < plan.getPathsNum(); i++) {
            try {
                if (!sessionPool.checkTimeseriesExists(PREFIX + plan.getPath(i))) {
                    TSDataType dataType = TSDataType.deserialize((byte) Short.parseShort(plan.getAttributes(i).getOrDefault("DataType", "5")));
                    TSEncoding encoding = TSEncoding.deserialize((byte) Short.parseShort(plan.getAttributes(i).getOrDefault("Encoding", "9")));
                    CompressionType compressionType = CompressionType.deserialize((byte) Short.parseShort(plan.getAttributesList().get(i).getOrDefault("CompressionType", "0")));
                    sessionPool.createTimeseries(PREFIX + plan.getPath(i), dataType, encoding, compressionType);
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
            }
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            sessionPool.deleteTimeseries(plan.getPaths().stream().map(x -> PREFIX + x).collect(Collectors.toList()));
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            sessionPool.deleteData(plan.getPaths().stream().map(x -> PREFIX + x).collect(Collectors.toList()), plan.getStartTime(), plan.getEndTime());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            sessionPool.setStorageGroup(PREFIX + plan.getDatabaseName());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new NonDataPlanExecuteResult(SUCCESS, plan);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            sessionPool.deleteStorageGroup(PREFIX + plan.getDatabaseName());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
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
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> counts = new ArrayList<>();
            List<Object> sums = new ArrayList<>();
            for (String path : plan.getPaths()) {
                String deviceId = path.substring(0, path.lastIndexOf('.'));
                String measurement = path.substring(path.lastIndexOf('.') + 1);
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(AVG, measurement, measurement, deviceId, plan.getStartTime(), plan.getEndTime()));
                RowRecord rowRecord = dataSet.next();
                dataTypeList.add(fromIoTDB(rowRecord.getFields().get(1).getDataType()));
                counts.add(rowRecord.getFields().get(0).getLongV());
                sums.add(rowRecord.getFields().get(1).getObjectValue(rowRecord.getFields().get(1).getDataType()));
                dataSet.close();
            }
            AvgAggregateQueryPlanExecuteResult result = new AvgAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(plan.getPaths());
            result.setDataTypes(dataTypeList);
            result.setCounts(counts);
            result.setSums(sums);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new AvgAggregateQueryPlanExecuteResult(FAILURE, null);
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteCountQueryPlan(CountQueryPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            List<DataType> dataTypeList = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                String deviceId = path.substring(0, path.lastIndexOf('.'));
                String measurement = path.substring(path.lastIndexOf('.') + 1);
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(COUNT, measurement, deviceId, plan.getStartTime(), plan.getEndTime()));
                Field field = dataSet.next().getFields().get(0);
                dataTypeList.add(fromIoTDB(field.getDataType()));
                values.add(field.getObjectValue(field.getDataType()));
                dataSet.close();
            }
            StatisticsAggregateQueryPlanExecuteResult result = new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(plan.getPaths());
            result.setDataTypes(dataTypeList);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new StatisticsAggregateQueryPlanExecuteResult(FAILURE, null);
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteSumQueryPlan(SumQueryPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            List<DataType> dataTypeList = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                String deviceId = path.substring(0, path.lastIndexOf('.'));
                String measurement = path.substring(path.lastIndexOf('.') + 1);
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(SUM, measurement, deviceId, plan.getStartTime(), plan.getEndTime()));
                Field field = dataSet.next().getFields().get(0);
                dataTypeList.add(fromIoTDB(field.getDataType()));
                values.add(field.getObjectValue(field.getDataType()));
                dataSet.close();
            }
            StatisticsAggregateQueryPlanExecuteResult result = new StatisticsAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(plan.getPaths());
            result.setDataTypes(dataTypeList);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new StatisticsAggregateQueryPlanExecuteResult(FAILURE, null);
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstQueryPlan(FirstQueryPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                timestamps.add(-1L);
                String deviceId = path.substring(0, path.lastIndexOf('.'));
                String measurement = path.substring(path.lastIndexOf('.') + 1);
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(FIRST_VALUE, measurement, deviceId, plan.getStartTime(), plan.getEndTime()));
                Field field = dataSet.next().getFields().get(0);
                if (field.getStringValue().equals("null")) {
                    dataTypeList.add(BINARY);
                    values.add("null".getBytes());
                } else {
                    dataTypeList.add(fromIoTDB(field.getDataType()));
                    values.add(field.getObjectValue(field.getDataType()));
                }
                dataSet.close();
            }
            SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(plan.getPaths());
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, null);
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                timestamps.add(-1L);
                String deviceId = path.substring(0, path.lastIndexOf('.'));
                String measurement = path.substring(path.lastIndexOf('.') + 1);
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(LAST_VALUE, measurement, deviceId, plan.getStartTime(), plan.getEndTime()));
                Field field = dataSet.next().getFields().get(0);
                if (field.getStringValue().equals("null")) {
                    dataTypeList.add(BINARY);
                    values.add("null".getBytes());
                } else {
                    dataTypeList.add(fromIoTDB(field.getDataType()));
                    values.add(field.getObjectValue(field.getDataType()));
                }
                dataSet.close();
            }
            SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(plan.getPaths());
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, null);
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMaxQueryPlan(MaxQueryPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                timestamps.add(-1L);
                String deviceId = path.substring(0, path.lastIndexOf('.'));
                String measurement = path.substring(path.lastIndexOf('.') + 1);
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(MAX_VALUE, measurement, deviceId, plan.getStartTime(), plan.getEndTime()));
                Field field = dataSet.next().getFields().get(0);
                if (field.getStringValue().equals("null")) {
                    dataTypeList.add(BINARY);
                    values.add("null".getBytes());
                } else {
                    dataTypeList.add(fromIoTDB(field.getDataType()));
                    values.add(field.getObjectValue(field.getDataType()));
                }
                dataSet.close();
            }
            SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(plan.getPaths());
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, null);
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMinQueryPlan(MinQueryPlan plan) {
        SessionPool sessionPool = readSessionPools.get(plan.getStorageEngineId());
        try {
            List<DataType> dataTypeList = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            List<Object> values = new ArrayList<>();
            for (String path : plan.getPaths()) {
                timestamps.add(-1L);
                String deviceId = path.substring(0, path.lastIndexOf('.'));
                String measurement = path.substring(path.lastIndexOf('.') + 1);
                SessionDataSetWrapper dataSet =
                        sessionPool.executeQueryStatement(String.format(MIN_VALUE, measurement, deviceId, plan.getStartTime(), plan.getEndTime()));
                Field field = dataSet.next().getFields().get(0);
                if (field.getStringValue().equals("null")) {
                    dataTypeList.add(BINARY);
                    values.add("null".getBytes());
                } else {
                    dataTypeList.add(fromIoTDB(field.getDataType()));
                    values.add(field.getObjectValue(field.getDataType()));
                }
                dataSet.close();
            }
            SingleValueAggregateQueryPlanExecuteResult result = new SingleValueAggregateQueryPlanExecuteResult(SUCCESS, plan);
            result.setPaths(plan.getPaths());
            result.setDataTypes(dataTypeList);
            result.setTimes(timestamps);
            result.setValues(values);
            return result;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new SingleValueAggregateQueryPlanExecuteResult(FAILURE, null);
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleAvgQueryDataPlan(DownsampleAvgQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleCountQueryDataPlan(DownsampleCountQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleSumQueryDataPlan(DownsampleSumQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMaxQueryDataPlan(DownsampleMaxQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMinQueryDataPlan(DownsampleMinQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstQueryDataPlan(DownsampleFirstQueryPlan plan) {
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastQueryDataPlan(DownsampleLastQueryPlan plan) {
        return null;
    }
}
