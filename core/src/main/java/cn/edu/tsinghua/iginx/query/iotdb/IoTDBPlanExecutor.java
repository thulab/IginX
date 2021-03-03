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
package cn.edu.tsinghua.iginx.query.iotdb;

import cn.edu.tsinghua.iginx.core.db.DBType;
import cn.edu.tsinghua.iginx.metadata.DatabaseMeta;
import cn.edu.tsinghua.iginx.plan.*;
import cn.edu.tsinghua.iginx.query.AbstractPlanExecutor;
import cn.edu.tsinghua.iginx.query.entity.TimeSeriesDataSet;
import cn.edu.tsinghua.iginx.query.iotdb.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.query.result.*;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
            if (databaseMeta.getDbType() != DBType.IoTDB) {
                logger.warn("unexpected database: " + databaseMeta.getDbType());
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
        SessionPool sessionPool = writeSessionPools.get(plan.getDatabaseId());

        try {
            Tablet tablets = new Tablet(null, null);
            sessionPool.insertTablet(tablets);
        } catch (Exception e) {
            logger.error("insert records error: ", e);
        }
        return null;
    }

    protected QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan, Session session) throws Exception {
        SessionDataSet sessionDataSet = session.executeRawDataQuery(plan.getPaths(), plan.getStartTime(), plan.getEndTime());
        List<String> columns = sessionDataSet.getColumnNames();
        List<TSDataType> columnTypes = sessionDataSet.getColumnTypes();
        List<TimeSeriesDataSet> timeSeriesDataSets = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            timeSeriesDataSets.add(new TimeSeriesDataSet(columns.get(i), DataTypeTransformer.fromIoTDB(columnTypes.get(i))));
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
        SessionPool sessionPool = readSessionPools.get(plan.getDatabaseId());

        return null;
    }

    @Override
    protected AddColumnsPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan) {
        SessionPool sessionPool = writeSessionPools.get(plan.getDatabaseId());
        return null;
    }

    @Override
    protected DeleteColumnsPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        return null;
    }

    @Override
    protected DeleteDataInColumnsPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        return null;
    }

    @Override
    protected CreateDatabasePlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan) {
        return null;
    }

    @Override
    protected DropDatabasePlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan dropDatabasePlan) {
        return null;
    }
}
