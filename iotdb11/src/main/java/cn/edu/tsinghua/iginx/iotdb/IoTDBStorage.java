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

import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.ColumnDataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RowDataView;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.iotdb.query.entity.IoTDBQueryRowStream;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.toIoTDB;
import static cn.edu.tsinghua.iginx.query.result.PlanExecuteResult.FAILURE;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

public class IoTDBStorage implements IStorage {

    private static final int BATCH_SIZE = 10000;

    private static final String STORAGE_ENGINE = "iotdb11";

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";

    private static final String SESSION_POOL_SIZE = "sessionPoolSize";

    private static final String DEFAULT_USERNAME = "root";

    private static final String DEFAULT_PASSWORD = "root";

    private static final String DEFAULT_SESSION_POOL_SIZE = "100";

    private static final String PREFIX = "root.";

    private static final String QUERY_DATA = "SELECT %s FROM " + PREFIX + "%s WHERE time >= %d and time < %d";

    private final SessionPool sessionPool;

    private final StorageEngineMeta meta;

    private static final Logger logger = LoggerFactory.getLogger(IoTDBStorage.class);

    public IoTDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
        this.meta = meta;
        if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
            throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
        }
        if (!testConnection()) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
        sessionPool = createSessionPool();
    }

    private boolean testConnection() {
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
        String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);

        Session session = new Session(meta.getIp(), meta.getPort(), username, password);
        try {
            session.open(false);
            session.close();
        } catch (IoTDBConnectionException e) {
            logger.error("test connection error: {}", e.getMessage());
            return false;
        }
        return true;
    }

    private SessionPool createSessionPool() {
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
        String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
        int sessionPoolSize = Integer.parseInt(extraParams.getOrDefault(SESSION_POOL_SIZE, DEFAULT_SESSION_POOL_SIZE));
        return new SessionPool(meta.getIp(), meta.getPort(), username, password, sessionPoolSize);
    }

    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) {
        List<Operator> operators = task.getOperators();
        if (operators.size() != 1) {
            return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
        }
        FragmentMeta fragment = task.getTargetFragment();
        Operator op = operators.get(0);
        String storageUnit = task.getStorageUnit();

        if (op.getType() == OperatorType.Project) { // 目前只实现 project 操作符
            Project project = (Project) op;
            return executeProjectTask(fragment.getTimeInterval(), fragment.getTsInterval(), storageUnit, project);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executeInsertTask(storageUnit, insert);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
        }
        return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
    }

    @Override
    public boolean supportsProject() {
        return true;
    }

    @Override
    public boolean supportsProjectAndSelect() {
        return true;
    }

    private TaskExecuteResult executeProjectTask(TimeInterval timeInterval, TimeSeriesInterval tsInterval, String storageUnit, Project project) { // 未来可能要用 tsInterval 对查询出来的数据进行过滤
        try {
            StringBuilder builder = new StringBuilder();
            for (String path : project.getPatterns()) {
                builder.append(path);
                builder.append(',');
            }
            String statement = String.format(QUERY_DATA, builder.deleteCharAt(builder.length() - 1).toString(), storageUnit, timeInterval.getStartTime(), timeInterval.getEndTime());
            System.out.println(statement);
            RowStream rowStream = new IoTDBQueryRowStream(sessionPool.executeQueryStatement(statement));
            return new TaskExecuteResult(rowStream);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute project task in iotdb11 failure", e));
        }
    }

    private TaskExecuteResult executeInsertTask(String storageUnit, Insert insert) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
                e = insertRowRecords((RowDataView) dataView, storageUnit);
                break;
            case Column:
                e = insertNonAlignedRowRecords((RowDataView) dataView, storageUnit);
                break;
            case NonAlignedRow:
                e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
                break;
            case NonAlignedColumn:
                e = insertNonAlignedColumnRecords((ColumnDataView) dataView, storageUnit);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null, new PhysicalException("execute insert task in iotdb11 failure", e));
        }
        return new TaskExecuteResult(null, null);
    }

    private Exception insertRowRecords(RowDataView data, String storageUnit) {
        Map<String, Tablet> tablets = new HashMap<>();
        Map<String, List<MeasurementSchema>> schemasMap = new HashMap<>();
        Map<String, List<Integer>> deviceIdToPathIndexes = new HashMap<>();
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);

        // 创建 tablets
        for (int i = 0; i < data.getPathNum(); i++) {
            String path = data.getPath(i);
            String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
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
            schemaList.add(new MeasurementSchema(measurement, toIoTDB(data.getDataType(i))));
            schemasMap.put(deviceId, schemaList);
            pathIndexes.add(i);
            deviceIdToPathIndexes.put(deviceId, pathIndexes);
        }

        for (Map.Entry<String, List<MeasurementSchema>> entry : schemasMap.entrySet()) {
            tablets.put(entry.getKey(), new Tablet(entry.getKey(), entry.getValue(), batchSize));
        }

        int cnt = 0;
        do {
            int size = Math.min(data.getTimeSize() - cnt, batchSize);
            // 对于每个时间戳，需要记录每个 deviceId 对应的 tablet 的 row 的变化
            Map<String, Integer> deviceIdToRow = new HashMap<>();

            // 插入 timestamps 和 values
            for (int i = cnt; i < cnt + size; i++) {
                int index = 0;
                deviceIdToRow.clear();
                for (int j = 0; j < data.getPathNum(); j++) {
                    BitmapView bitmapView = data.getBitmapView(i);
                    if (bitmapView.get(j)) {
                        String path = data.getPath(j);
                        String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
                        String measurement = path.substring(path.lastIndexOf('.') + 1);
                        Tablet tablet = tablets.get(deviceId);
                        if (!deviceIdToRow.containsKey(deviceId)) {
                            int row = tablet.rowSize++;
                            tablet.addTimestamp(row, data.getTimestamp(i));
                            deviceIdToRow.put(deviceId, row);
                        }
                        if (data.getDataType(j) == BINARY) {
                            tablet.addValue(measurement, deviceIdToRow.get(deviceId), new Binary((byte[]) data.getValue(i, index)));
                        } else {
                            tablet.addValue(measurement, deviceIdToRow.get(deviceId), data.getValue(i, index));
                        }
                        index++;
                    }
                }
            }

            try {
                sessionPool.insertTablets(tablets);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
                return e;
            }

            for (Tablet tablet : tablets.values()) {
                tablet.reset();
            }
            cnt += size;
        } while(cnt < data.getTimeSize());

        return null;
    }

    private Exception insertNonAlignedRowRecords(RowDataView data, String storageUnit) {
        return null;
    }

    private Exception insertColumnRecords(ColumnDataView data, String storageUnit) {
        Map<String, Tablet> tablets = new HashMap<>();
        Map<String, List<MeasurementSchema>> schemasMap = new HashMap<>();
        Map<String, List<Integer>> deviceIdToPathIndexes = new HashMap<>();
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);

        // 创建 tablets
        for (int i = 0; i < data.getPathNum(); i++) {
            String path = data.getPath(i);
            String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
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
            schemaList.add(new MeasurementSchema(measurement, toIoTDB(data.getDataType(i))));
            schemasMap.put(deviceId, schemaList);
            pathIndexes.add(i);
            deviceIdToPathIndexes.put(deviceId, pathIndexes);
        }

        for (Map.Entry<String, List<MeasurementSchema>> entry : schemasMap.entrySet()) {
            tablets.put(entry.getKey(), new Tablet(entry.getKey(), entry.getValue(), batchSize));
        }

        int cnt = 0;
        int[] indexes = new int[data.getPathNum()];
        do {
            int size = Math.min(data.getTimeSize() - cnt, batchSize);

            // 插入 timestamps 和 values
            for (Map.Entry<String, List<Integer>> entry : deviceIdToPathIndexes.entrySet()) {
                String deviceId = entry.getKey();
                Tablet tablet = tablets.get(deviceId);
                for (int i = cnt; i < cnt + size; i++) {
                    BitmapView bitmapView = data.getBitmapView(entry.getValue().get(0));
                    if (bitmapView.get(i)) {
                        int row = tablet.rowSize++;
                        tablet.addTimestamp(row, data.getTimestamp(i));
                        for (Integer j : entry.getValue()) {
                            String path = data.getPath(j);
                            String measurement = path.substring(path.lastIndexOf('.') + 1);
                            if (data.getDataType(j) == BINARY) {
                                tablet.addValue(measurement, row, new Binary((byte[]) data.getValue(j, indexes[j])));
                            } else {
                                tablet.addValue(measurement, row, data.getValue(j, indexes[j]));
                            }
                            indexes[j]++;
                        }
                    }
                }
            }

            try {
                sessionPool.insertTablets(tablets);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.error(e.getMessage());
                return e;
            }

            for (Tablet tablet : tablets.values()) {
                tablet.reset();
            }
            cnt += size;
        } while(cnt < data.getTimeSize());

        return null;
    }

    private Exception insertNonAlignedColumnRecords(ColumnDataView data, String storageUnit) {
        return null;
    }

    private TaskExecuteResult executeDeleteTask(String storageUnit, Delete delete) {
        return null;
    }

}
