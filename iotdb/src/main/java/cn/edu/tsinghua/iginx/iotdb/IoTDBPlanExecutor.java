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
import cn.edu.tsinghua.iginx.metadata.DatabaseMeta;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DropDatabasePlan;
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.query.AbstractPlanExecutor;
import cn.edu.tsinghua.iginx.query.entity.TimeSeriesDataSet;
import cn.edu.tsinghua.iginx.query.result.AddColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.CreateDatabasePlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DeleteColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DeleteDataInColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DropDatabasePlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.InsertRecordsPlanExecuteResult;
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
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.fromIoTDB;
import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.toIoTDB;

public class IoTDBPlanExecutor extends AbstractPlanExecutor {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBPlanExecutor.class);

    private final Map<Long, DatabaseMeta> databaseMetas;

    private Map<Long, SessionPool> readSessionPools;

    private Map<Long, SessionPool> writeSessionPools;

    public IoTDBPlanExecutor(List<DatabaseMeta> databaseMetaList) {
        readSessionPools = new HashMap<>();
        writeSessionPools = new HashMap<>();
        databaseMetas = new HashMap<>();
        for (DatabaseMeta databaseMeta: databaseMetaList) {
            if (databaseMeta.getStorageEngine() != StorageEngine.IoTDB) {
                logger.warn("unexpected database: " + databaseMeta.getStorageEngine());
                continue;
            }
            Map<String, String> extraParams = databaseMeta.getExtraParams();
            String username = extraParams.getOrDefault("username", "root");
            String password = extraParams.getOrDefault("password", "root");
            int readSessions = Integer.parseInt(extraParams.getOrDefault("readSessions", "2"));
            int writeSessions = Integer.parseInt(extraParams.getOrDefault("writeSessions", "5"));
            SessionPool readSessionPool = new SessionPool(databaseMeta.getIp(), databaseMeta.getPort(), username, password, readSessions);
            SessionPool writeSessionPool = new SessionPool(databaseMeta.getIp(), databaseMeta.getPort(), username, password, writeSessions);
            readSessionPools.put(databaseMeta.getId(), readSessionPool);
            writeSessionPools.put(databaseMeta.getId(), writeSessionPool);
            databaseMetas.put(databaseMeta.getId(), databaseMeta);
        }
        readSessionPools = Collections.unmodifiableMap(readSessionPools);
        writeSessionPools = Collections.unmodifiableMap(writeSessionPools);
    }

    @Override
    protected InsertRecordsPlanExecuteResult syncExecuteInsertRecordsPlan(InsertRecordsPlan plan) {
        logger.info("执行插入计划！");
        SessionPool sessionPool = writeSessionPools.get(plan.getDatabaseId());

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

        return new InsertRecordsPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    protected QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan, Session session) throws Exception {
        SessionDataSet sessionDataSet = session.executeRawDataQuery(plan.getPaths(), plan.getStartTime(), plan.getEndTime());
        List<String> columns = sessionDataSet.getColumnNames();
        List<TSDataType> columnTypes = sessionDataSet.getColumnTypes();
        List<TimeSeriesDataSet> timeSeriesDataSets = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            timeSeriesDataSets.add(new TimeSeriesDataSet(columns.get(i), fromIoTDB(columnTypes.get(i))));
        }
        while (sessionDataSet.hasNext()) {
            RowRecord record = sessionDataSet.next();
            long timestamp = record.getTimestamp();
            List<Field> fields = record.getFields();
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (field.isNull())
                    continue;
                switch (columnTypes.get(i)) {
                    case INT32:
                    case INT64:
                        timeSeriesDataSets.get(i).addDataPoint(timestamp, field.getIntV());
                        break;
                    case DOUBLE:
                        timeSeriesDataSets.get(i).addDataPoint(timestamp, field.getDoubleV());
                        break;
                    case FLOAT:
                        timeSeriesDataSets.get(i).addDataPoint(timestamp, field.getFloatV());
                        break;
                    case BOOLEAN:
                        timeSeriesDataSets.get(i).addDataPoint(timestamp, field.getBoolV());
                        break;
                    case TEXT:
                        timeSeriesDataSets.get(i).addDataPoint(timestamp, field.getBinaryV());
                        break;
                }
            }
        }
        sessionDataSet.closeOperationHandle();
        return new QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, plan, timeSeriesDataSets);
    }

    @Override
    protected QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        DatabaseMeta databaseMeta = databaseMetas.get(plan.getDatabaseId());
        Map<String, String> extraParams = databaseMeta.getExtraParams();
        String username = extraParams.getOrDefault("username", "root");
        String password = extraParams.getOrDefault("password", "root");
        Session session = new Session(databaseMeta.getIp(), databaseMeta.getPort(), username, password);
        try {
            session.open();
            return syncExecuteQueryDataPlan(plan, session);
        } catch (Exception e) {
            logger.error("query data error: ", e);
        } finally {
            try {
                session.close();
            } catch (Exception e) {
                logger.error("got error:", e);
            }
        }
        return new QueryDataPlanExecuteResult(PlanExecuteResult.FAILURE, plan, null);
    }

    @Override
    protected AddColumnsPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getDatabaseId());
        for (int i = 0; i < plan.getPathsNum(); i++) {
            try {
                if (!sessionPool.checkTimeseriesExists(plan.getPath(i))) {
                    TSDataType dataType = TSDataType.deserialize((byte) Short.parseShort(plan.getAttributes().get(i).getOrDefault("DataType", "5")));
                    TSEncoding encoding = TSEncoding.deserialize((byte) Short.parseShort(plan.getAttributes().get(i).getOrDefault("Encoding", "9")));
                    CompressionType compressionType = CompressionType.deserialize((byte) Short.parseShort(plan.getAttributes().get(i).getOrDefault("CompressionType", "0")));
                    sessionPool.createTimeseries(plan.getPath(i), dataType, encoding, compressionType);
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
            }
        }
        return new AddColumnsPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    @Override
    protected DeleteColumnsPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getDatabaseId());
        try {
            sessionPool.deleteTimeseries(plan.getPaths());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new DeleteColumnsPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    @Override
    protected DeleteDataInColumnsPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getDatabaseId());
        try {
            sessionPool.deleteData(plan.getPaths(), plan.getStartTime(), plan.getEndTime());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new DeleteDataInColumnsPlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    @Override
    protected CreateDatabasePlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getDatabaseId());
        try {
            sessionPool.setStorageGroup(plan.getDatabaseName());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new CreateDatabasePlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }

    @Override
    protected DropDatabasePlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getDatabaseId());
        try {
            sessionPool.deleteStorageGroup(plan.getDatabaseName());
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        return new DropDatabasePlanExecuteResult(PlanExecuteResult.SUCCESS, plan);
    }
}
