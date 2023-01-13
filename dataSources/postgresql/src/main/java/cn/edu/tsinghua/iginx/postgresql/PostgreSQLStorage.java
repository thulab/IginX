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
package cn.edu.tsinghua.iginx.postgresql;
import java.util.HashMap;
import java.util.Date;

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
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.TimeFilter;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.postgresql.entity.PostgreSQLQueryRowStream;
import cn.edu.tsinghua.iginx.postgresql.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.postgresql.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.postgresql.tools.TagFilterUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLStorage implements IStorage {

  private static final Logger logger = LoggerFactory.getLogger(PostgreSQLStorage.class);

  private static final int BATCH_SIZE = 10000;

  private static final String STORAGE_ENGINE = "postgresql";

  private static final String USERNAME = "username";

  private static final String PASSWORD = "password";

  private static final String DBNAME = "dbname";

  private static final String DEFAULT_USERNAME = "postgres";

  private static final String DEFAULT_PASSWORD = "postgres";

  private static final String DEFAULT_DBNAME = "wy";

  private static final String QUERY_DATABASES = "SELECT datname FROM pg_database";

  private static final String FIRST_QUERY = "select first(%s, time) from %s";

  private static final String LAST_QUERY = "select last(%s, time) from %s";

  private static final String QUERY_DATA = "SELECT time, %s FROM %s WHERE %s and %s";

  private static final String DELETE_DATA = "DELETE FROM %s WHERE time >= to_timestamp(%d) and time < to_timestamp(%d)";

  private static final String IGINX_SEPARATOR = ".";

  private static final String POSTGRESQL_SEPARATOR = "$";

  private static final String DATABASE_PREFIX = "unit";

  private static final long MAX_TIMESTAMP = Integer.MAX_VALUE;

  private final StorageEngineMeta meta;

  private Connection connection;

  public PostgreSQLStorage(StorageEngineMeta meta) throws StorageInitializationException {
    this.meta = meta;
    if (!testConnection()) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
    String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
    String connUrl = String
        .format("jdbc:postgresql://%s:%s/?user=%s&password=%s", meta.getIp(), meta.getPort(),
            username, password);
    try {
      System.out.println("init----------------------");
      connection = DriverManager.getConnection(connUrl);
      Statement stmt = connection.createStatement();
      ResultSet tableSet = stmt.executeQuery("select * from pg_tables");
      String s="";
      String s1="";
      String s2="";
      while (tableSet.next()) {
        String table=tableSet.getString(2);
        if (true) {
          stmt = connection.createStatement();
//          System.out.print("begin!!! init----");
          s1="create sequence t start with 1 increment by 1000 no minvalue no maxvalue cache 1";
          s = String.format("alter table %s add time timestamp", table);
          s2=String.format("alter table %s set time=to_timestamp(1000) where time=null",table);
//          System.out.println(s);
//          System.out.println("temporary test!!!");
          try {
            stmt.execute(s);
          }catch (SQLException e) {
            logger.info("pass!!!");
          }
          try{
            stmt.execute(s1);
          }catch (SQLException e) {
            logger.info("pass 2  ");
          }
          try{
            stmt.execute(s2);
          }catch (SQLException e){
            logger.info("pass 3  ");
          }
//          System.out.println("end1111111111");
        }
      }
      //System.out.println("init end---------------------------");
    } catch (SQLException e) {
      throw new StorageInitializationException("cannot connect to " + meta.toString());
    }
  }

  private boolean hasNoTime(Connection connection,String table) {
    try {
      boolean flag=true;
      DatabaseMetaData databaseMetaData=connection.getMetaData();
      ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, "%");
      while (columnSet.next()) {
        String columnName = columnSet.getString("COLUMN_NAME");//获取列名称
//        System.out.println(table+"   "+columnName);
        if (columnName.equals("time5")) {

          flag = false;
          break;
        }
      }
      return flag;
    } catch (SQLException e) {
      return true;
    }
  }

  private boolean testConnection() {
    Map<String, String> extraParams = meta.getExtraParams();
    String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
    String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
    String connUrl = String
        .format("jdbc:postgresql://%s:%s/?user=%s&password=%s", meta.getIp(), meta.getPort(),
            username, password);
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
      //System.out.println("init----------------------");
      //connection = DriverManager.getConnection(connUrl);
//    try {
//      Statement stmt = connection.createStatement();
//      ResultSet tableSet = stmt.executeQuery("select * from pg_tables");
//
//      String s="";
//      String s1="";
//      String s2="";
//      while (tableSet.next()) {
//        String table=tableSet.getString(2);
//        if (true) {
//          stmt = connection.createStatement();
////          System.out.print("begin!!! init----");
//          s1="create sequence t start with 1 increment by 1000 no minvalue no maxvalue cache 1";
//          s = String.format("alter table %s add time7 integer", table);
//          s2=String.format("alter table %s set time=to_timestamp(1000) where time=null",table);
////          System.out.println(s);
////          System.out.println("temporary test!!!");
//          try {
//            stmt.execute(s);
//          }catch (SQLException e) {
//            logger.info("pass!!!");
//          }
//          try{
//            stmt.execute(s1);
//          }catch (SQLException e) {
//            logger.info("pass 2  ");
//          }
//          try{
//            stmt.execute(s2);
//          }catch (SQLException e){
//            logger.info("pass 3  ");
//          }
////          System.out.println("end1111111111");
//        }
//      }
//    }catch (SQLException e) {
//      throw new StorageInitializationException("cannot connect to " + meta.toString());
//      logger.info("here");
//    }
    logger.info("111111111111111111111");
    List<Operator> operators = task.getOperators();
    logger.info("222222222222222222222",operators);
//    logger.info(operators);
    if (operators.size() != 1) {
      return new TaskExecuteResult(
          new NonExecutablePhysicalTaskException("unsupported physical task"));
    }
    FragmentMeta fragment = task.getTargetFragment();
    logger.info("3333333333333333333333",fragment);
    Operator op = operators.get(0);
    logger.info("4444444444444444444444",op);
    String storageUnit = task.getStorageUnit();
    logger.info("5555555555555555555555",storageUnit);
    // 先切换数据库
    useDatabase(storageUnit);

    if (op.getType() == OperatorType.Project) { // 目前只实现 project 操作符
      Project project = (Project) op;
      Filter filter;
      if (operators.size() == 2) {
        filter = ((Select) operators.get(1)).getFilter();
        logger.info("select!!!!!!!!!");
      } else {
        logger.info("and !!!!!!!!!");
        filter = new AndFilter(Arrays
            .asList(new TimeFilter(Op.GE, fragment.getTimeInterval().getStartTime()),
                new TimeFilter(Op.L, fragment.getTimeInterval().getEndTime())));
      }
      return executeProjectTask(project, filter);
    } else if (op.getType() == OperatorType.Insert) {
      Insert insert = (Insert) op;
      logger.info("insert !!!!!!!!!");
      return executeInsertTask(insert);
    } else if (op.getType() == OperatorType.Delete) {
      Delete delete = (Delete) op;
      logger.info("delete !!!!!!!!!");
      return executeDeleteTask(delete);
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
        if (tableName.startsWith("unit")) {
          tableName = tableName.substring(tableName.indexOf(POSTGRESQL_SEPARATOR) + 1);
        }
        while (columnSet.next()) {
          String columnName = columnSet.getString("COLUMN_NAME");//获取列名称
          String typeName = columnSet.getString("TYPE_NAME");//列字段类型
          timeseries.add(new Timeseries(
              tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                  + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR),
              DataTypeTransformer.fromPostgreSQL(typeName)));
        }
      }
    } catch (SQLException e) {
      throw new PhysicalException(e);
    }
    return timeseries;
  }

  @Override
  public Pair<TimeSeriesInterval, TimeInterval> getBoundaryOfStorage() throws PhysicalException {
    long minTime = Long.MAX_VALUE, maxTime = 0;
    List<String> paths = new ArrayList<>();
    try {
      Statement stmt = connection.createStatement();
      ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES);
      while (databaseSet.next()) {
        String databaseName = databaseSet.getString(1);//获取表名称
        if (databaseName.startsWith(DATABASE_PREFIX)) {
          useDatabase(databaseName);
          DatabaseMetaData databaseMetaData = connection.getMetaData();
          ResultSet tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
          while (tableSet.next()) {
            String tableName = tableSet.getString(3);//获取表名称
            ResultSet columnSet = databaseMetaData.getColumns(null, "%", tableName, "%");
            while (columnSet.next()) {
              String columnName = columnSet.getString("COLUMN_NAME");//获取列名称
              paths.add(tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                  + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR));
              // 获取first
              String firstQueryStatement = String.format(FIRST_QUERY, columnName, tableName);
              Statement firstQueryStmt = connection.createStatement();
              ResultSet firstQuerySet = firstQueryStmt.executeQuery(firstQueryStatement);
              if (firstQuerySet.next()) {
                long currMinTime = firstQuerySet.getLong(1);
                minTime = Math.min(currMinTime, minTime);
              }
              // 获取last
              String lastQueryStatement = String.format(LAST_QUERY, columnName, tableName);
              Statement lastQueryStmt = connection.createStatement();
              ResultSet lastQuerySet = lastQueryStmt.executeQuery(lastQueryStatement);
              if (lastQuerySet.next()) {
                long currMaxTime = lastQuerySet.getLong(1);
                maxTime = Math.max(currMaxTime, maxTime);
              }
            }
          }
        }
      }
    } catch (SQLException e) {
      throw new PhysicalException(e);
    }
    paths.sort(String::compareTo);

    return new Pair<>(new TimeSeriesInterval(paths.get(0), paths.get(paths.size() - 1)),
 //       new TimeInterval(minTime, maxTime + 1));
            new TimeInterval(0, Long.MAX_VALUE));
  }

  private TaskExecuteResult executeProjectTask(Project project,
      Filter filter) { // 未来可能要用 tsInterval 对查询出来的数据进行过滤
    logger.info("select task begin");
    try {
      logger.info("select task 1111111111111");
      List<ResultSet> resultSets = new ArrayList<>();
      List<Field> fields = new ArrayList<>();
      for (String path : project.getPatterns()) {
        String table = path.substring(0, path.lastIndexOf('.'));
        table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
        String field = path.substring(path.lastIndexOf('.') + 1);
        field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
        // 查询序列类型
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, field);
        System.out.println(table+"  "+field);
        logger.info("test here---------");
//        if(field.contains("*")){
//          columnSet = databaseMetaData.getColumns(null, null, table, null);
//        }
        String a="*";
        if(field.contains(a)){
          logger.info("test1----");
//          String typeName = columnSet.getString("TYPE_NAME");//列字段类型
//          System.out.println(typeName);
//          fields
//                  .add(new Field(table.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
//                          + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
//                          , DataTypeTransformer.fromPostgreSQL(typeName)));
//          System.out.println(fields);
////          String statement = String
////              .format(QUERY_DATA, field, table,
////                  TagFilterUtils.transformToFilterStr(project.getTagFilter()),
////                  FilterTransformer.toString(filter));
//          String statement=String.format("select %s from %s",field,table);
////          System.out.println(fields);
//          System.out.println(statement);
//          logger.info("test 2---");
//          Statement stmt = connection.createStatement();
//          ResultSet rs = stmt.executeQuery(statement);
//          logger.info("test3-------");
//          resultSets.add(rs);
//          System.out.println(resultSets);
          field="id";
//          String typeName="int";
          columnSet = databaseMetaData.getColumns(null, "%", table, field);
        }
        if (columnSet.next()) {
          logger.info("test1----");
          String typeName = columnSet.getString("TYPE_NAME");//列字段类型
          System.out.println(typeName);
          fields
              .add(new Field(table.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                  + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                  , DataTypeTransformer.fromPostgreSQL(typeName)));
          System.out.println(fields);
//          String statement = String
//              .format(QUERY_DATA, field, table,
//                  TagFilterUtils.transformToFilterStr(project.getTagFilter()),
//                  FilterTransformer.toString(filter));
         String statement=String.format("select time,%s from %s",field,table);
//          System.out.println(fields);
          System.out.println(statement);
          logger.info("test 2---");
          Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(statement);
          logger.info("test3-------");
          resultSets.add(rs);
          System.out.println(resultSets);
        }
//        else {
//          String statement = String.format("select %s from %s", field, table);
//          Statement stmt = connection.createStatement();
//          ResultSet rs = stmt.executeQuery(statement);
//          logger.info("test 4----");
//        }
      }
      RowStream rowStream = new PostgreSQLQueryRowStream(resultSets, fields);
      logger.info("test 5---------");
      return new TaskExecuteResult(rowStream);
    } catch (SQLException e) {
      return new TaskExecuteResult(
          new PhysicalTaskExecuteFailureException("execute project task in postgresql failure",
              e));
    }
  }

  private TaskExecuteResult executeInsertTask(Insert insert) {
    logger.info("execute insertTask!!!!!!!!!!!!!!!!!!");
    DataView dataView = insert.getData();
    Exception e = null;
    switch (dataView.getRawDataType()) {
      case Row:
      case NonAlignedRow:
        logger.info("nonalignedrow!!!!!!!!!!!!!!");
        e = insertRowRecords((RowDataView) dataView);
        break;
      case Column:
      case NonAlignedColumn:
        logger.info("column!!!!!!!!!!!!!!!!!!!!!!!!!");
        e = insertColumnRecords((ColumnDataView) dataView);
        break;
    }
    if (e != null) {
      return new TaskExecuteResult(null,
          new PhysicalException("execute insert task in pg failure", e));
    }
    logger.info("null!!!!!!!!!!!!!!!!!!!!!!!!!!");
    return new TaskExecuteResult(null, null);
  }

  private void createTimeSeriesIfNotExists(String table, String field,
      Map<String, String> tags, DataType dataType) {
    logger.info("create time series begin");
    try {
      DatabaseMetaData databaseMetaData = connection.getMetaData();
      logger.info("ct 1");
      ResultSet tableSet = databaseMetaData.getTables(null, "%", table, new String[]{"TABLE"});
      logger.info("ct 2");
      if (!tableSet.next()) {
        logger.info("ct 3");
        Statement stmt = connection.createStatement();
        StringBuilder stringBuilder = new StringBuilder();
        logger.info("ct 3");
        for (Entry<String, String> tagsEntry : tags.entrySet()) {
          stringBuilder.append(tagsEntry.getKey()).append(" TEXT,");
        }
        logger.info("ct 4");
        stringBuilder.append(field).append(" ").append(DataTypeTransformer.toPostgreSQL(dataType));
        stmt.execute(String
            .format("CREATE TABLE %s (time TIMESTAMPTZ NOT NULL,%s NULL)", table,
                stringBuilder.toString()));
        logger.info("ct 5");
      } else {
        logger.info("ct 6");
        for (String tag : tags.keySet()) {
          ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, tag);
          if (!columnSet.next()) {
            Statement stmt = connection.createStatement();
            stmt.execute(String.format("ALTER TABLE %s ADD COLUMN %s TEXT NULL", table, tag));
          }
        }
        logger.info("ct 7");
        ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, field);
        logger.info("ct 8");
        if (!columnSet.next()) {
          logger.info("ct 9");
          Statement stmt = connection.createStatement();
          stmt.execute(String.format("ALTER TABLE %s ADD COLUMN %s %s NULL", table, field,
              DataTypeTransformer.toPostgreSQL(dataType)));
          logger.info("ct 10");
        }
      }
    } catch (SQLException e) {
      logger.error("create timeseries error", e);
    }
  }

  private void useDatabase(String dbname) {
//    try {
//      Statement stmt = connection.createStatement();
//      stmt.execute(String.format("create database %s", dbname));
//    } catch (SQLException e) {
//      logger.info("create database error", e);
//    }
//    try {
//      Map<String, String> extraParams = meta.getExtraParams();
//      String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
//      String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
//      String connUrl = String
//          .format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", meta.getIp(), meta.getPort(),
//              dbname, username, password);
//      connection = DriverManager.getConnection(connUrl);
//    } catch (SQLException e) {
//      logger.info("change database error", e);
//    }
  }

  private Exception insertRowRecords(RowDataView data) {
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
            String table = path.substring(0, path.lastIndexOf('.'));
            table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
            String field = path.substring(path.lastIndexOf('.') + 1);
            field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
            Map<String, String> tags = data.getTags(i);
            createTimeSeriesIfNotExists(table, field, tags, dataType);

            long time = data.getTimestamp(i) / 1000; // timescaledb存10位时间戳，java为13位时间戳
            String value;
            if (data.getDataType(j) == DataType.BINARY) {
              value = "'" + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8)
                  + "'";
            } else {
              value = data.getValue(i, index).toString();
            }

            StringBuilder columnsKeys = new StringBuilder();
            StringBuilder columnValues = new StringBuilder();
            for (Entry<String, String> tagEntry : tags.entrySet()) {
              columnsKeys.append(tagEntry.getValue()).append(" ");
              columnValues.append(tagEntry.getValue()).append(" ");
            }
            columnsKeys.append(field);
            columnValues.append(value);
            System.out.println(columnsKeys);
            System.out.println(columnValues);
            stmt.addBatch(String
                .format("INSERT INTO %s (time, %s) values (to_timestamp(%d), %s)", table,
                    columnsKeys, time, columnValues));
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

  private Exception insertColumnRecords(ColumnDataView data) {
    logger.info("insert column 11111111111111");
    int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
    logger.info("insert column 333333333333333");
    try {
      Statement stmt = connection.createStatement();
      logger.info("insert column 44444444444444");
      for (int i = 0; i < data.getPathNum(); i++) {
        logger.info("insert 55555555555555");
        String path = data.getPath(i);
        logger.info("insert column 1");
        DataType dataType = data.getDataType(i);
        logger.info("insert column 2");
        String table = path.substring(0, path.lastIndexOf('.'));
        logger.info("2");
        table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
        logger.info("insert col  3");
        String field = path.substring(path.lastIndexOf('.') + 1);
        logger.info("insert column 4");
        field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
        logger.info("insert colum  5");
        Map<String, String> tags = data.getTags(i);
        if (tags == null) {
          tags = new HashMap<>();
        }
        System.out.println(tags);
        logger.info("insert column 6");
        createTimeSeriesIfNotExists(table, field, tags, dataType);     //error
        logger.info("insert column 7");
        BitmapView bitmapView = data.getBitmapView(i);
        logger.info("insert column 8");
        int index = 0;
        for (int j = 0; j < data.getTimeSize(); j++) {
          logger.info("insert column 9");
          if (bitmapView.get(j)) {
            long time = data.getTimestamp(j) / 1000; // timescaledb存10位时间戳，java为13位时间戳
            String value;
            if (data.getDataType(i) == DataType.BINARY) {
              value = "'" + new String((byte[]) data.getValue(i, index), StandardCharsets.UTF_8)
                  + "'";
            } else {
              value = data.getValue(i, index).toString();
            }
            logger.info("insert column 10");
            StringBuilder columnsKeys = new StringBuilder();
            StringBuilder columnValues = new StringBuilder();
            for (Entry<String, String> tagEntry : tags.entrySet()) {
              columnsKeys.append(tagEntry.getValue()).append(" ");
              columnValues.append(tagEntry.getValue()).append(" ");
            }
            columnsKeys.append(field);
            columnValues.append(value);
            logger.info("insert column 2222222222222222222222");
            System.out.println(table+"  "+columnsKeys+"   "+time+"   "+columnValues);
            stmt.addBatch(String
                .format("INSERT INTO %s (time, %s) values (to_timestamp(%d), %s)", table,
                    columnsKeys,
                    time,
                    columnValues));
            if (index > 0 && (index + 1) % batchSize == 0) {
              stmt.executeBatch();
            }
            index++;
          }
        }
      }
      stmt.executeBatch();
    } catch (SQLException e) {
      logger.info("insert column error!!!!!!!!!!!!!!!!!!!!!!!!");
      return e;
    }

    return null;
  }

  private TaskExecuteResult executeDeleteTask(Delete delete) {
    // only support to the level of device now
    // TODO support the delete to the level of sensor
    try {
      for (int i = 0; i < delete.getPatterns().size(); i++) {
        String path = delete.getPatterns().get(i);
        TimeRange timeRange = delete.getTimeRanges().get(i);
        String table = path.substring(0, path.lastIndexOf('.'));
        table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
        String field = path.substring(path.lastIndexOf('.') + 1);
        field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
        // 查询序列类型
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, field);
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

  @Override
  public void release() throws PhysicalException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new PhysicalException(e);
    }
  }
}