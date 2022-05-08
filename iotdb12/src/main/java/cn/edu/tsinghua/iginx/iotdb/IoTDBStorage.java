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
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
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
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
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
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer.toIoTDB;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

public class IoTDBStorage implements IStorage {

    private static final int BATCH_SIZE = 10000;

    private static final String STORAGE_ENGINE = "iotdb12";

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";

    private static final String SESSION_POOL_SIZE = "sessionPoolSize";

    private static final String DEFAULT_USERNAME = "root";

    private static final String DEFAULT_PASSWORD = "root";

    private static final String DEFAULT_SESSION_POOL_SIZE = "100";

    private static final String PREFIX = "root.";

    private static final String QUERY_DATA = "SELECT %s FROM " + PREFIX + "%s WHERE time >= %d and time < %d";

    private static final String DELETE_STORAGE_GROUP_CLAUSE = "DELETE STORAGE GROUP " + PREFIX + "%s";

    private static final String DELETE_TIMESERIES_CLAUSE = "DELETE TIMESERIES " + PREFIX + "%s";

    private static final String SHOW_TIMESERIES = "SHOW TIMESERIES";

    private static final String DOES_NOT_EXISTED = "does not exist";

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
            return executeDeleteTask(storageUnit, delete);
        }
        return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
    }

    @Override
    public List<Timeseries> getTimeSeries(String timeSeriesPrefix) throws PhysicalException {
        List<Timeseries> timeseries = new ArrayList<>();
        try {
            String statement = SHOW_TIMESERIES;
            if(timeSeriesPrefix.length() > 0){
                statement += " root." + timeSeriesPrefix;
            }
            SessionDataSetWrapper dataSet = sessionPool.executeQueryStatement(statement);
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
                        timeseries.add(new Timeseries(path, DataType.BOOLEAN));
                        break;
                    case "FLOAT":
                        timeseries.add(new Timeseries(path, DataType.FLOAT));
                        break;
                    case "TEXT":
                        timeseries.add(new Timeseries(path, DataType.BINARY));
                        break;
                    case "DOUBLE":
                        timeseries.add(new Timeseries(path, DataType.DOUBLE));
                        break;
                    case "INT32":
                        timeseries.add(new Timeseries(path, DataType.INTEGER));
                        break;
                    case "INT64":
                        timeseries.add(new Timeseries(path, DataType.LONG));
                        break;
                }
            }
            dataSet.close();
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new PhysicalTaskExecuteFailureException("get time series failure: ", e);
        }
        return timeseries;
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
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute project task in iotdb12 failure", e));
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
                e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
                break;
            case NonAlignedRow:
                e = insertNonAlignedRowRecords((RowDataView) dataView, storageUnit);
                break;
            case NonAlignedColumn:
                e = insertNonAlignedColumnRecords((ColumnDataView) dataView, storageUnit);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null, new PhysicalException("execute insert task in iotdb12 failure", e));
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
                BitmapView bitmapView = data.getBitmapView(i);
                for (int j = 0; j < data.getPathNum(); j++) {
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
        Map<Integer, Map<String, Tablet>> tabletsMap = new HashMap<>();
        Map<Integer, Integer> pathIndexToTabletIndex = new HashMap<>();
        Map<String, Integer> deviceIdToCnt = new HashMap<>();
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);

        // 创建 tablets
        for (int i = 0; i < data.getPathNum(); i++) {
            String path = data.getPath(i);
            String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
            String measurement = path.substring(path.lastIndexOf('.') + 1);
            int measurementNum;
            Map<String, Tablet> tablets;

            measurementNum = deviceIdToCnt.computeIfAbsent(deviceId, x -> -1);
            deviceIdToCnt.put(deviceId, measurementNum + 1);
            pathIndexToTabletIndex.put(i, measurementNum + 1);
            tablets = tabletsMap.computeIfAbsent(measurementNum + 1, x -> new HashMap<>());
            tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(data.getDataType(i)))), batchSize));
            tabletsMap.put(measurementNum + 1, tablets);
        }

        int cnt = 0;
        do {
            int size = Math.min(data.getTimeSize() - cnt, batchSize);
            boolean[] needToInsert = new boolean[tabletsMap.size()];
            Arrays.fill(needToInsert, false);

            // 插入 timestamps 和 values
            for (int i = cnt; i < cnt + size; i++) {
                int index = 0;
                BitmapView bitmapView = data.getBitmapView(i);
                for (int j = 0; j < data.getPathNum(); j++) {
                    if (bitmapView.get(j)) {
                        String path = data.getPath(j);
                        String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
                        String measurement = path.substring(path.lastIndexOf('.') + 1);
                        Tablet tablet = tabletsMap.get(pathIndexToTabletIndex.get(j)).get(deviceId);
                        int row = tablet.rowSize++;
                        tablet.addTimestamp(row, data.getTimestamp(i));
                        if (data.getDataType(j) == BINARY) {
                            tablet.addValue(measurement, row, new Binary((byte[]) data.getValue(i, index)));
                        } else {
                            tablet.addValue(measurement, row, data.getValue(i, index));
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
                return e;
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
        } while(cnt < data.getTimeSize());

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
        Map<Integer, Map<String, Tablet>> tabletsMap = new HashMap<>();
        Map<Integer, List<Integer>> tabletIndexToPathIndexes = new HashMap<>();
        Map<String, Integer> deviceIdToCnt = new HashMap<>();
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);

        // 创建 tablets
        for (int i = 0; i < data.getPathNum(); i++) {
            String path = data.getPath(i);
            String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
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
            tablets.put(deviceId, new Tablet(deviceId, Collections.singletonList(new MeasurementSchema(measurement, toIoTDB(data.getDataType(i)))), batchSize));
            tabletsMap.put(measurementNum + 1, tablets);
        }

        for (Map.Entry<Integer, List<Integer>> entry : tabletIndexToPathIndexes.entrySet()) {
            int cnt = 0;
            int[] indexesOfBitmap = new int[entry.getValue().size()];
            do {
                int size = Math.min(data.getTimeSize() - cnt, batchSize);

                // 插入 timestamps 和 values
                for (int i = 0; i < entry.getValue().size(); i++) {
                    int index = entry.getValue().get(i);
                    String path = data.getPath(index);
                    String deviceId = PREFIX + storageUnit + "." + path.substring(0, path.lastIndexOf('.'));
                    String measurement = path.substring(path.lastIndexOf('.') + 1);
                    Tablet tablet = tabletsMap.get(entry.getKey()).get(deviceId);
                    BitmapView bitmapView = data.getBitmapView(index);
                    for (int j = cnt; j < cnt + size; j++) {
                        if (bitmapView.get(j)) {
                            int row = tablet.rowSize++;
                            tablet.addTimestamp(row, data.getTimestamp(j));
                            if (data.getDataType(index) == BINARY) {
                                tablet.addValue(measurement, row, new Binary((byte[]) data.getValue(index, indexesOfBitmap[i])));
                            } else {
                                tablet.addValue(measurement, row, data.getValue(index, indexesOfBitmap[i]));
                            }
                            indexesOfBitmap[i]++;
                        }
                    }
                }

                try {
                    sessionPool.insertTablets(tabletsMap.get(entry.getKey()));
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    logger.error(e.getMessage());
                    return e;
                }

                for (Tablet tablet : tabletsMap.get(entry.getKey()).values()) {
                    tablet.reset();
                }
                cnt += size;
            } while(cnt < data.getTimeSize());
        }
        return null;
    }

    private TaskExecuteResult executeDeleteTask(String storageUnit, Delete delete) {
        if (delete.getTimeRanges() == null || delete.getTimeRanges().size() == 0) { // 没有传任何 time range
            List<String> paths = delete.getPatterns();
            if (paths.size() == 1 && paths.get(0).equals("*")) {
                try {
                    sessionPool.executeNonQueryStatement(String.format(DELETE_STORAGE_GROUP_CLAUSE, storageUnit));
                } catch (IoTDBConnectionException | StatementExecutionException e) {
                    logger.warn("encounter error when clear data: " + e.getMessage());
                    if (!e.getMessage().contains(DOES_NOT_EXISTED)) {
                        return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute clear data in iotdb12 failure", e));
                    }
                }
            } else {
                for (String path: paths) {
                    try {
                        sessionPool.executeNonQueryStatement(String.format(DELETE_TIMESERIES_CLAUSE, storageUnit + "." + path));
                    } catch (IoTDBConnectionException | StatementExecutionException e) {
                        logger.warn("encounter error when delete path: " + e.getMessage());
                        if (!e.getMessage().contains(DOES_NOT_EXISTED)) {
                            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute delete path task in iotdb12 failure", e));
                        }
                    }
                }
            }
        } else {
            try {
                List<String> paths = delete.getPatterns().stream().map(x -> PREFIX + storageUnit + "." + x).collect(Collectors.toList());
                for (TimeRange timeRange: delete.getTimeRanges()) {
                    sessionPool.deleteData(paths, timeRange.getActualBeginTime(), timeRange.getActualEndTime());
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                logger.warn("encounter error when delete data: " + e.getMessage());
                if (!e.getMessage().contains(DOES_NOT_EXISTED)) {
                    return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("execute delete data task in iotdb12 failure", e));
                }
            }
        }
        return new TaskExecuteResult(null, null);
    }

}