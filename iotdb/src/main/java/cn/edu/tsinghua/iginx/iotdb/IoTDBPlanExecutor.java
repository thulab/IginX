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
import cn.edu.tsinghua.iginx.metadatav2.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DropDatabasePlan;
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.query.AbstractPlanExecutor;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.toIoTDB;

public class IoTDBPlanExecutor extends AbstractPlanExecutor {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBPlanExecutor.class);

    private final Map<Long, StorageEngineMeta> storageEngineMetas;

    private Map<Long, SessionPool> readSessionPools;

    private Map<Long, SessionPool> writeSessionPools;

    public IoTDBPlanExecutor(List<StorageEngineMeta> storageEngineMetaList) {
        readSessionPools = new HashMap<>();
        writeSessionPools = new HashMap<>();
        storageEngineMetas = new HashMap<>();
        for (StorageEngineMeta storageEngineMeta: storageEngineMetaList) {
            if (storageEngineMeta.getDbType() != StorageEngine.IoTDB) {
                logger.warn("unexpected database: " + storageEngineMeta.getDbType());
                continue;
            }
            Map<String, String> extraParams = storageEngineMeta.getExtraParams();
            String username = extraParams.getOrDefault("username", "root");
            String password = extraParams.getOrDefault("password", "root");
            int readSessions = Integer.parseInt(extraParams.getOrDefault("readSessions", "2"));
            int writeSessions = Integer.parseInt(extraParams.getOrDefault("writeSessions", "5"));
            SessionPool readSessionPool = new SessionPool(storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password, readSessions);
            SessionPool writeSessionPool = new SessionPool(storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password, writeSessions);
            readSessionPools.put(storageEngineMeta.getId(), readSessionPool);
            writeSessionPools.put(storageEngineMeta.getId(), writeSessionPool);
            storageEngineMetas.put(storageEngineMeta.getId(), storageEngineMeta);
        }
        readSessionPools = Collections.unmodifiableMap(readSessionPools);
        writeSessionPools = Collections.unmodifiableMap(writeSessionPools);
    }

    @Override
    protected NonDataPlanExecuteResult syncExecuteInsertRecordsPlan(InsertRecordsPlan plan) {
        logger.info("执行插入计划！");
        SessionPool sessionPool = writeSessionPools.get(plan.getStorageEngineId());

        Map<String, Tablet> tablets = new HashMap<>();
        Map<String, List<String>> measurementsMap= new HashMap<>();
        Map<String, List<TSDataType>> typesMap = new HashMap<>();

        // 解析 deviceId 和 measurement
        for (int i = 0; i < plan.getPathsNum(); i++) {
            String deviceId = plan.getPath(i).substring(0, plan.getPath(i).lastIndexOf('.'));
            String measurement = plan.getPath(i).substring(plan.getPath(i).lastIndexOf('.') + 1);
            TSDataType dataType = toIoTDB(plan.getDataType(i));

            measurementsMap.computeIfAbsent(deviceId, k -> new ArrayList<>());
            measurementsMap.get(deviceId).add(measurement);

            typesMap.computeIfAbsent(deviceId, k -> new ArrayList<>());
            typesMap.get(deviceId).add(dataType);
        }

        // 创建 schema
        for (Map.Entry<String, List<String>> measurements : measurementsMap.entrySet()) {
            List<MeasurementSchema> measurementSchemaList = new ArrayList<>();
            for (int i = 0; i < measurements.getValue().size(); i++) {
                measurementSchemaList.add(new MeasurementSchema(measurements.getValue().get(i), typesMap.get(measurements.getKey()).get(i)));
            }
            tablets.put(measurements.getKey(), new Tablet(measurements.getKey(), measurementSchemaList));
        }

        // 插入 timestamps
        for (int i = 0; i < plan.getTimestamps().length; i++) {
            for (Map.Entry<String, Tablet> tablet : tablets.entrySet()) {
                int row = tablet.getValue().rowSize++;
                tablet.getValue().addTimestamp(row, plan.getTimestamp(i));
            }
        }

        // 插入 values
        for (int i = 0; i < plan.getValuesList().length; i++) {
            Object[] values = (Object[]) plan.getValuesList()[i];
            String deviceId = plan.getPath(i).substring(0, plan.getPath(i).lastIndexOf('.'));
            String measurement = plan.getPath(i).substring(plan.getPath(i).lastIndexOf('.') + 1);
            for (int j = 0; j < values.length; j++) {
                tablets.get(deviceId).addValue(measurement, j, values[j]);
            }
        }

        try {
            sessionPool.insertTablets(tablets);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }

        return new NonDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    protected QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan, Session session) throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet sessionDataSet = session.executeRawDataQuery(plan.getPaths(), plan.getStartTime(), plan.getEndTime());
        return new QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan, new IoTDBQueryExecuteDataSet(sessionDataSet, session));
    }

    @Override
    protected QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        StorageEngineMeta storageEngineMeta = storageEngineMetas.get(plan.getStorageEngineId());
        Map<String, String> extraParams = storageEngineMeta.getExtraParams();
        String username = extraParams.getOrDefault("username", "root");
        String password = extraParams.getOrDefault("password", "root");
        Session session = new Session(storageEngineMeta.getIp(), storageEngineMeta.getPort(), username, password);
        try {
            session.open();
            return syncExecuteQueryDataPlan(plan, session);
        } catch (Exception e) {
            logger.error("query data error: ", e);
        }
        return new QueryDataPlanExecuteResult(PlanExecuteResult.FAILURE, plan, null);
    }

    @Override
    protected NonDataPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getStorageEngineId());
        for (int i = 0; i < plan.getPathsNum(); i++) {
            try {
                if (!sessionPool.checkTimeseriesExists(plan.getPath(i))) {
                    TSDataType dataType = TSDataType.deserialize((byte) Short.parseShort(plan.getAttributes(i).getOrDefault("DataType", "5")));
                    TSEncoding encoding = TSEncoding.deserialize((byte) Short.parseShort(plan.getAttributes(i).getOrDefault("Encoding", "9")));
                    CompressionType compressionType = CompressionType.deserialize((byte) Short.parseShort(plan.getAttributesList().get(i).getOrDefault("CompressionType", "0")));
                    sessionPool.createTimeseries(plan.getPath(i), dataType, encoding, compressionType);
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
            }
        }
        return new NonDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    @Override
    protected NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getStorageEngineId());
        try {
            sessionPool.deleteTimeseries(plan.getPaths());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new NonDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    @Override
    protected NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getStorageEngineId());
        try {
            sessionPool.deleteData(plan.getPaths(), plan.getStartTime(), plan.getEndTime());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new NonDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    @Override
    protected NonDataPlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getStorageEngineId());
        try {
            sessionPool.setStorageGroup(plan.getDatabaseName());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new NonDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    @Override
    protected NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getStorageEngineId());
        try {
            sessionPool.deleteStorageGroup(plan.getDatabaseName());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new NonDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }
}
