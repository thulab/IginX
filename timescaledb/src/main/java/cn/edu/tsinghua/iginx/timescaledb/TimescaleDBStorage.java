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
package cn.edu.tsinghua.iginx.timescaledb;

import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
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
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.timescaledb.entity.TimescaleDBQueryRowStream;
import cn.edu.tsinghua.iginx.timescaledb.tools.DataTypeTransformer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimescaleDBStorage implements IStorage {

  private static final Logger logger = LoggerFactory.getLogger(TimescaleDBStorage.class);

  private static final int BATCH_SIZE = 10000;

  private static final String STORAGE_ENGINE = "timescaledb";

  private static final String USERNAME = "username";

  private static final String PASSWORD = "password";

  private static final String DBNAME = "dbname";

  private static final String DEFAULT_USERNAME = "postgres";

  private static final String DEFAULT_PASSWORD = "123456";

  private static final String DEFAULT_DBNAME = "timeseries";

  private static final String QUERY_DATA = "SELECT time, %s FROM %s WHERE time >= to_timestamp(%d) and time < to_timestamp(%d)";

  private static final String DELETE_DATA = "DELETE FROM %s WHERE time >= to_timestamp(%d) and time < to_timestamp(%d)";

  private static final String IGINX_SEPARATOR = ".";

  private static final String TIMESCALEDB_SEPARATOR = "$";

  private static final long MAX_TIMESTAMP = Integer.MAX_VALUE;

  private final StorageEngineMeta meta;

  private final Connection connection;

  public TimescaleDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    if (!testConnection()) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
    String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
    String dbName = extraParams.getOrDefault(DBNAME, DEFAULT_DBNAME);
    String connUrl = String
        .format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", meta.getIp(), meta.getPort(),
            dbName, username, password);
    try {
      connection = DriverManager.getConnection(connUrl);
    } catch (SQLException e) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
  }

  private boolean testConnection() {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
    String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
    String dbName = extraParams.getOrDefault(DBNAME, DEFAULT_DBNAME);
    String connUrl = String
        .format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", meta.getIp(), meta.getPort(),
            dbName, username, password);
    try {
      Class.forName("org.postgresql.Driver");
      DriverManager.getConnection(connUrl);
      return true;
    } catch (SQLException | ClassNotFoundException e) {
      return false;
    }
  }

  @Override
  public TaskExecuteResult execute(StoragePhysicalTask task) {
    List<Operator> operators = task.getOperators();
    if (operators.size() != 1) {
      return new TaskExecuteResult(
          new NonExecutablePhysicalTaskException("unsupported physical task"));
    }
    FragmentMeta fragment = task.getTargetFragment();
    Operator op = operators.get(0);
    String storageUnit = task.getStorageUnit();

    if (op.getType() == OperatorType.Project) { // 目前只实现 project 操作符
      Project project = (Project) op;
      return executeProjectTask(fragment.getTimeInterval(), fragment.getTsInterval(), storageUnit,
          project);
    } else if (op.getType() == OperatorType.Insert) {
      Insert insert = (Insert) op;
      return executeInsertTask(storageUnit, insert);
    } else if (op.getType() == OperatorType.Delete) {
      Delete delete = (Delete) op;
      return executeDeleteTask(storageUnit, delete);
    }
    return new TaskExecuteResult(
        new NonExecutablePhysicalTaskException("unsupported physical task"));
  }

  @Override
  public List<Timeseries> getTimeSeries() throws PhysicalException {
    List<Timeseries> timeseries = new ArrayList<>();
    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      ResultSet tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
      while (tableSet.next()) {
        String tableName = tableSet.getString(3);//获取表名称
        ResultSet columnSet = databaseMetaData.getColumns(null, "%", tableName, "%");
        while (columnSet.next()) {
          String columnName = columnSet.getString("COLUMN_NAME");//获取列名称
          String typeName = columnSet.getString("TYPE_NAME");//列字段类型
          timeseries.add(new Timeseries(
              tableName.replace(TIMESCALEDB_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                  + columnName.replace(TIMESCALEDB_SEPARATOR, IGINX_SEPARATOR),
              DataTypeTransformer.fromTimescaleDB(typeName)));
        }
      }
    } catch (SQLException e) {
      throw new PhysicalException(e);
    }
    return timeseries;
  }

  private TaskExecuteResult executeProjectTask(TimeInterval timeInterval,
      TimeSeriesInterval tsInterval, String storageUnit,
      Project project) { // 未来可能要用 tsInterval 对查询出来的数据进行过滤
    try {
      List<ResultSet> resultSets = new ArrayList<>();
      List<Field> fields = new ArrayList<>();
      for (String path : project.getPatterns()) {
        String table = storageUnit + IGINX_SEPARATOR + path.substring(0, path.lastIndexOf('.'));
        table = table.replace(IGINX_SEPARATOR, TIMESCALEDB_SEPARATOR);
        String sensor = path.substring(path.lastIndexOf('.') + 1);
        sensor = sensor.replace(IGINX_SEPARATOR, TIMESCALEDB_SEPARATOR);
        // 查询序列类型
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, sensor);
        if (columnSet.next()) {
          String typeName = columnSet.getString("TYPE_NAME");//列字段类型
          fields
              .add(new Field(table.replace(TIMESCALEDB_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                  + sensor.replace(TIMESCALEDB_SEPARATOR, IGINX_SEPARATOR)
                  , DataTypeTransformer.fromTimescaleDB(typeName)));
          String statement = String
              .format(QUERY_DATA, sensor, table,
                  timeInterval.getStartTime(), Math.min(timeInterval.getEndTime(), MAX_TIMESTAMP));
          Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(statement);
          resultSets.add(rs);
        }
      }
      RowStream rowStream = new TimescaleDBQueryRowStream(resultSets, fields);
      return new TaskExecuteResult(rowStream);
    } catch (SQLException e) {
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in timescaledb failure",
              e));
    }
  }

  private TaskExecuteResult executeInsertTask(String storageUnit, Insert insert) {
    DataView dataView = insert.getData();
    Exception e = null;
    switch (dataView.getRawDataType()) {
      case Row:
      case NonAlignedRow:
        e = insertRowRecords((RowDataView) dataView, storageUnit);
        break;
      case Column:
      case NonAlignedColumn:
        e = insertColumnRecords((ColumnDataView) dataView, storageUnit);
        break;
    }
    if (e != null) {
      return new TaskExecuteResult(null,
          new PhysicalException("execute insert task in iotdb12 failure", e));
    }
    return new TaskExecuteResult(null, null);
  }

  private void createTimeSeriesIfNotExists(String table, String column, DataType dataType) {
    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      ResultSet tableSet = databaseMetaData.getTables(null, "%", table, new String[]{"TABLE"});
      if (!tableSet.next()) {
        Statement stmt = connection.createStatement();
        stmt.execute(String
            .format("CREATE TABLE %s (time TIMESTAMPTZ NOT NULL,%s %s NULL)", table, column,
                DataTypeTransformer.toTimescaleDB(dataType)));
        stmt.execute(String.format("SELECT create_hypertable('%s', 'time')", table));
      } else {
        ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, column);
        if (!columnSet.next()) {
          Statement stmt = connection.createStatement();
          stmt.execute(String.format("ALTER TABLE %s ADD COLUMN %s %s NULL", table, column,
              DataTypeTransformer.toTimescaleDB(dataType)));
        }
      }
    } catch (SQLException e) {
      logger.error("create timeseries error", e);
    }
  }

  private Exception insertRowRecords(RowDataView data, String storageUnit) {
    int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
    try {
      Statement stmt = connection.createStatement();
      for (int i = 0; i < data.getTimeSize(); i++) {
        BitmapView bitmapView = data.getBitmapView(i);
        int index = 0;
        for (int j = 0; j < data.getPathNum(); j++) {
          if (bitmapView.get(j)) {
            String path = data.getPath(j);
            DataType dataType = data.getDataType(j);
            String table = storageUnit + IGINX_SEPARATOR + path.substring(0, path.lastIndexOf('.'));
            table = table.replace(IGINX_SEPARATOR, TIMESCALEDB_SEPARATOR);
            String sensor = path.substring(path.lastIndexOf('.') + 1);
            sensor = sensor.replace(IGINX_SEPARATOR, TIMESCALEDB_SEPARATOR);
            createTimeSeriesIfNotExists(table, sensor, dataType);

            long time = data.getTimestamp(i) / 1000; // timescaledb存10位时间戳，java为13位时间戳
            String value = data.getValue(i, index).toString();
            stmt.addBatch(String
                .format("INSERT INTO %s (time, %s) values (to_timestamp(%d), %s)", table, sensor,
                    time,
                    value));
            if (index > 0 && (index + 1) % batchSize == 0) {
              stmt.executeBatch();
            }

            index++;
          }
        }
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      return e;
    }

    return null;
  }

  private Exception insertColumnRecords(ColumnDataView data, String storageUnit) {
    int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
    try {
      Statement stmt = connection.createStatement();
      for (int i = 0; i < data.getPathNum(); i++) {
        String path = data.getPath(i);
        DataType dataType = data.getDataType(i);
        String table = storageUnit + IGINX_SEPARATOR + path.substring(0, path.lastIndexOf('.'));
        table = table.replace(IGINX_SEPARATOR, TIMESCALEDB_SEPARATOR);
        String sensor = path.substring(path.lastIndexOf('.') + 1);
        sensor = sensor.replace(IGINX_SEPARATOR, TIMESCALEDB_SEPARATOR);
        createTimeSeriesIfNotExists(table, sensor, dataType);
        BitmapView bitmapView = data.getBitmapView(i);
        int index = 0;
        for (int j = 0; j < data.getTimeSize(); j++) {
          if (bitmapView.get(j)) {
            long time = data.getTimestamp(j) / 1000; // timescaledb存10位时间戳，java为13位时间戳
            String value = data.getValue(i, index).toString();
            stmt.addBatch(String
                .format("INSERT INTO %s (time, %s) values (to_timestamp(%d), %s)", table, sensor,
                    time,
                    value));
            if (index > 0 && (index + 1) % batchSize == 0) {
              stmt.executeBatch();
            }
            index++;
          }
        }
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      return e;
    }

    return null;
  }

  private TaskExecuteResult executeDeleteTask(String storageUnit, Delete delete) {
    // only support to the level of device now
    // TODO support the delete to the level of sensor
    try {
      for (int i = 0; i < delete.getPatterns().size(); i++) {
        String path = delete.getPatterns().get(i);
        TimeRange timeRange = delete.getTimeRanges().get(i);
        String table = storageUnit + IGINX_SEPARATOR + path.substring(0, path.lastIndexOf('.'));
        table = table.replace(IGINX_SEPARATOR, TIMESCALEDB_SEPARATOR);
        String sensor = path.substring(path.lastIndexOf('.') + 1);
        sensor = sensor.replace(IGINX_SEPARATOR, TIMESCALEDB_SEPARATOR);
        // 查询序列类型
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, sensor);
        if (columnSet.next()) {
          String statement = String
              .format(DELETE_DATA, table,
                  timeRange.getBeginTime(), Math.min(timeRange.getEndTime(), MAX_TIMESTAMP));
          Statement stmt = connection.createStatement();
          stmt.execute(statement);
        }
      }
      return new TaskExecuteResult(null, null);
    } catch (SQLException e) {
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute delete task in timescaledb failure",
              e));
    }
  }

}