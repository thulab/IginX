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
package cn.edu.tsinghua.iginx.postgresql_wy;

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
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.TimeFilter;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.postgresql_wy.query.entity.PostgreSQLQueryRowStream;
import cn.edu.tsinghua.iginx.postgresql_wy.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.postgresql_wy.tools.FilterTransformer;
import cn.edu.tsinghua.iginx.postgresql_wy.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

public class PostgreSQLStorage implements IStorage {

    private static final int BATCH_SIZE = 10000;

    private static final String STORAGE_ENGINE = "postgresql";

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";

    private static final String DEFAULT_USERNAME = "postgres";

    private static final String QUERY_DATABASES = "SELECT datname FROM pg_database";

    private static final String DATABASE_PREFIX = "unit";

    private static final long MAX_TIMESTAMP = Integer.MAX_VALUE;

    private static final String DEFAULT_PASSWORD = "postgres";

    private static final String DELETE_DATA = "DELETE FROM %s WHERE time >= to_timestamp(%d) and time < to_timestamp(%d)";

    private static final String PREFIX = "postgres.";

    private static final String QUERY_DATA = "SELECT %s FROM " + PREFIX + "%s WHERE %s";

    private static final String QUERY_HISTORY_DATA = "SELECT %s FROM postgres WHERE %s";

    private final StorageEngineMeta meta;

    private static final String IGINX_SEPARATOR = ".";

    private static final String POSTGRESQL_SEPARATOR = "$";

    private Connection connection;

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLStorage.class);

    public PostgreSQLStorage(StorageEngineMeta meta) throws StorageInitializationException {
        this.meta = meta;
        if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
            throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
        }
        if (!testConnection()) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
        Map<String, String> extraParams = meta.getExtraParams();
        String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
        String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
        String connUrl = String.format("jdbc:postgresql://%s:%s/?user=%s&password=%s", meta.getIp(), meta.getPort(),username, password);
        try {
            System.out.println("init----------------------");
          connection = DriverManager.getConnection(connUrl);
          Statement stmt = connection.createStatement();
          ResultSet tableSet = stmt.executeQuery("select * from pg_tables");
          String s="";
          while (tableSet.next()) {
            String table=tableSet.getString(2);
            if (hasNoTime(connection,table)) {
              stmt = connection.createStatement();
              s = String.format("alter table %s add time timestamp", table);
              stmt.execute(s);
            }
          }
          System.out.println("init end---------------------------");
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
                System.out.println(table+"   "+columnName)
                if (columnName == "time") {
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
        String connUrl = String.format("jdbc:postgresql://%s:%s/?user=%s&password=%s", meta.getIp(), meta.getPort(), username, password);
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
        System.out.println("init----------------------");
//        connection = DriverManager.getConnection(connUrl);
        Statement stmt = connection.createStatement();
        ResultSet tableSet = stmt.executeQuery("select * from pg_tables");
        String s="";
        while (tableSet.next()) {
            String table=tableSet.getString(2);
            if (hasNoTime(connection,table)) {
                stmt = connection.createStatement();
                s = String.format("alter table %s add time timestamp", table);
                stmt.execute(s);
            }}
        System.out.println("init end----------------------");
        List<Operator> operators = task.getOperators();
        if (operators.size() != 1) {
            return new TaskExecuteResult(
                    new NonExecutablePhysicalTaskException("unsupported physical task"));
        }
        FragmentMeta fragment = task.getTargetFragment();
        Operator op = operators.get(0);
        String storageUnit = task.getStorageUnit();
        // 先切换数据库
        useDatabase(storageUnit);

        if (op.getType() == OperatorType.Project) { // 目前只实现 project 操作符
            Project project = (Project) op;
            Filter filter;
            if (operators.size() == 2) {
                filter = ((Select) operators.get(1)).getFilter();
            } else {
                filter = new AndFilter(Arrays
                        .asList(new TimeFilter(Op.GE, fragment.getTimeInterval().getStartTime()),
                                new TimeFilter(Op.L, fragment.getTimeInterval().getEndTime())));
            }
            return executeQueryTask(project, filter);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executeInsertTask(insert);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
            return executeDeleteTask(delete);
        }
        return new TaskExecuteResult(
                new NonExecutablePhysicalTaskException("unsupported physical task"));
    }


    public Pair<TimeSeriesInterval, TimeInterval> getBoundaryOfStorage() throws PhysicalException {
        long minTime = Long.MAX_VALUE, maxTime = 0;
        List<String> paths = new ArrayList<>();
        List<Long> primaryToTimestamp=new ArrayList<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet databaseSet = stmt.executeQuery(QUERY_DATABASES);
            String primarykeyName="";
            while (databaseSet.next()) {
                String databaseName = databaseSet.getString(1);//获取表名称
                if (databaseName.startsWith(DATABASE_PREFIX)) {
                    useDatabase(databaseName);
                    DatabaseMetaData databaseMetaData = connection.getMetaData();
                    ResultSet tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
                    while (tableSet.next()) {
                        String tableName = tableSet.getString(3);//获取表名称
                        ResultSet columnSet = databaseMetaData.getColumns(null, "%", tableName, "%");
                        ResultSet primarykeySet=databaseMetaData.getPrimaryKeys(null,null,tableName);
                        while(primarykeySet.next()){
                            primarykeyName=primarykeySet.getString("COLUMN_NAME");
                            break;
                        }
                        String s="SELECT "+primarykeyName+" FROM "+tableName;
                        ResultSet primaryValue=stmt.executeQuery(s);
                        while(primaryValue.next()) {
                            primaryToTimestamp.add(idToTimestamp(primaryValue.getString(1)));
                        }
                        while (columnSet.next()) {
                            String columnName = columnSet.getString("COLUMN_NAME");//获取列名称
                            paths.add(tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                    + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR));
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new PhysicalException(e);
        }
        paths.sort(String::compareTo);
        Collections.sort(primaryToTimestamp);

        return new Pair<>(new TimeSeriesInterval(paths.get(0), paths.get(paths.size() - 1)),
                new TimeInterval(primaryToTimestamp.get(0), primaryToTimestamp.get(primaryToTimestamp.size())-1));
    }

    private void useDatabase(String dbname) {
        try {
            Statement stmt = connection.createStatement();
            stmt.execute(String.format("create database %s", dbname));
        } catch (SQLException e) {
            logger.info("create database error", e);
        }
        try {
            Map<String, String> extraParams = meta.getExtraParams();
            String username = extraParams.getOrDefault(USERNAME, DEFAULT_USERNAME);
            String password = extraParams.getOrDefault(PASSWORD, DEFAULT_PASSWORD);
            String connUrl = String
                    .format("jdbc:postgresql://%s:%s/%s?user=%s&password=%s", meta.getIp(), meta.getPort(),
                            dbname, username, password);
            connection = DriverManager.getConnection(connUrl);
        } catch (SQLException e) {
            logger.info("change database error", e);
        }
    }

    private long idToTimestamp(String str) {
        long hash=0;
        long x=0;
        int i=0;
        while(str.charAt(i)!='\0')	{
            hash=(hash<<4)+(long)(str.charAt(i));
            if((x=hash & 0xf000000000000000L)!=0) {
                hash^=(x>>24);
                hash&=~x;
            }
            i++;
        }
        return (hash & 0x7fffffffffffffffL);
    }

    @Override
    public void release() throws PhysicalException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new PhysicalException(e);
        }
    }

    @Override
    public List<Timeseries> getTimeSeries() throws PhysicalException {
        System.out.println("init----------------------");
//        connection = DriverManager.getConnection(connUrl);
        Statement stmt = connection.createStatement();
        ResultSet tableSet = stmt.executeQuery("select * from pg_tables");
        String s="";
        while (tableSet.next()) {
            String table=tableSet.getString(2);
            if (hasNoTime(connection,table)) {
                stmt = connection.createStatement();
                s = String.format("alter table %s add time timestamp", table);
                stmt.execute(s);
            }}
        System.out.println("init end----------------------");
        List<Timeseries> timeseries = new ArrayList<>();
        String primaryColumnName="";
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet tableSet = databaseMetaData.getTables(null, "%", "%", new String[]{"TABLE"});
            while (tableSet.next()) {
                String tableName = tableSet.getString(3);//获取表名称
                ResultSet columnSet = databaseMetaData.getColumns(null, "%", tableName, "%");
                if (tableName.startsWith("unit")) {
                    tableName = tableName.substring(tableName.indexOf(POSTGRESQL_SEPARATOR) + 1);
                }
                ResultSet primaryColumnNames=databaseMetaData.getPrimaryKeys(null,null,tableName);
                while(primaryColumnNames.next()){
                    primaryColumnName=primaryColumnNames.getString(1);
                }
                while (columnSet.next()) {
                    String columnName = columnSet.getString("COLUMN_NAME");//获取列名称
                    String typeName = columnSet.getString("TYPE_NAME");//列字段类型
                    if(columnName.equals(primaryColumnName)){
                        timeseries.add(new Timeseries(
                                tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                        + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR),
                                DataType.LONG));
                    }
                    else {
                        timeseries.add(new Timeseries(
                                tableName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                        + columnName.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR),
                                DataTypeTransformer.fromPostgreSQL(typeName)));
                    }
                }
            }
        } catch (SQLException e) {
            throw new PhysicalException(e);
        }
        return timeseries;
    }

    private TaskExecuteResult executeQueryTask(Project project, Filter filter) {
        try {
        List<ResultSet> resultSets = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        String primarykeyName="";
        List<Long> primarykeyValueList=new ArrayList<>();
        for (String path : project.getPatterns()) {
            String table = path.substring(0, path.lastIndexOf('.'));
            table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
            String field = path.substring(path.lastIndexOf('.') + 1);
            field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
            // 查询序列类型
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet primarykeySet=databaseMetaData.getPrimaryKeys(null,null,table);
            while(primarykeySet.next()){
                primarykeyName=primarykeySet.getString("COLUMN_NAME");                           //主键列名,时间戳相关
                break;
            }
            Statement stmt = connection.createStatement();
            String s="SELECT "+primarykeyName+" FROM "+table;
            ResultSet primaryValue=stmt.executeQuery(s);
            while(primaryValue.next()) {
                primarykeyValueList.add(idToTimestamp(primaryValue.getString(1)));
            }
            ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, field);
            if (columnSet.next()) {
                String typeName = columnSet.getString("TYPE_NAME");//列字段类型
                fields
                        .add(new Field(table.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                , DataTypeTransformer.fromPostgreSQL(typeName)));
                String statement = String
                        .format(QUERY_DATA, field, table,
                                TagKVUtils.transformToFilterStr(project.getTagFilter()),
                                FilterTransformer.toString(filter));
                //Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(statement);
                resultSets.add(rs);
            }
        }
        RowStream rowStream = new PostgreSQLQueryRowStream(resultSets, fields,primarykeyValueList);
        return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
        return new TaskExecuteResult(
                new PhysicalTaskExecuteFailureException("execute project task in postgresql failure",
                        e));
        }
    }

    private TaskExecuteResult executeQueryHistoryTask(Project project, Filter filter) {
        try {
            List<ResultSet> resultSets = new ArrayList<>();
            List<Field> fields = new ArrayList<>();
            String primarykeyName="";
            List<Long> primarykeyValueList=new ArrayList<>();
            for (String path : project.getPatterns()) {
                String table = path.substring(0, path.lastIndexOf('.'));
                table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String field = path.substring(path.lastIndexOf('.') + 1);
                field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                // 查询序列类型
                DatabaseMetaData databaseMetaData = connection.getMetaData();
                ResultSet primarykeySet=databaseMetaData.getPrimaryKeys(null,null,table);
                while(primarykeySet.next()){
                    primarykeyName=primarykeySet.getString("COLUMN_NAME");                           //主键列名,时间戳相关
                    break;
                }
                Statement stmt = connection.createStatement();
                String s="SELECT "+primarykeyName+" FROM "+table;
                ResultSet primaryValue=stmt.executeQuery(s);
                while(primaryValue.next()) {
                    primarykeyValueList.add(idToTimestamp(primaryValue.getString(1)));
                }
                ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, field);
                if (columnSet.next()) {
                    String typeName = columnSet.getString("TYPE_NAME");//列字段类型
                    fields
                            .add(new Field(table.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR) + IGINX_SEPARATOR
                                    + field.replace(POSTGRESQL_SEPARATOR, IGINX_SEPARATOR)
                                    , DataTypeTransformer.fromPostgreSQL(typeName)));
                    String statement = String
                            .format(QUERY_HISTORY_DATA, field,
                                    TagKVUtils.transformToFilterStr(project.getTagFilter()),
                                    FilterTransformer.toString(filter));
                    //Statement stmt = connection.createStatement();
                    ResultSet rs = stmt.executeQuery(statement);
                    resultSets.add(rs);
                }
            }
            RowStream rowStream = new PostgreSQLQueryRowStream(resultSets, fields,primarykeyValueList);
            return new TaskExecuteResult(rowStream);
        } catch (SQLException e) {
            return new TaskExecuteResult(
                    new PhysicalTaskExecuteFailureException("execute project task in postgresql failure",
                            e));
        }
    }

    private void createTimeSeriesIfNotExists(String table, String field, Map<String, String> tags, DataType dataType) {
        try {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            ResultSet tableSet = databaseMetaData.getTables(null, "%", table, new String[]{"TABLE"});
            if (!tableSet.next()) {
                Statement stmt = connection.createStatement();
                StringBuilder stringBuilder = new StringBuilder();
                for (Entry<String, String> tagsEntry : tags.entrySet()) {
                    stringBuilder.append(tagsEntry.getKey()).append(" TEXT,");
                }
                stringBuilder.append(field).append(" ").append(DataTypeTransformer.toPostgreSQL(dataType));
                stmt.execute(String
                        .format("CREATE TABLE %s (time TIMESTAMPTZ NOT NULL,%s NULL)", table,
                                stringBuilder.toString()));
            } else {
                for (String tag : tags.keySet()) {
                    ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, tag);
                    if (!columnSet.next()) {
                        Statement stmt = connection.createStatement();
                        stmt.execute(String.format("ALTER TABLE %s ADD COLUMN %s TEXT NULL", table, tag));
                    }
                }
                ResultSet columnSet = databaseMetaData.getColumns(null, "%", table, field);
                if (!columnSet.next()) {
                    Statement stmt = connection.createStatement();
                    stmt.execute(String.format("ALTER TABLE %s ADD COLUMN %s %s NULL", table, field,
                            DataTypeTransformer.toPostgreSQL(dataType)));
                }
            }
        } catch (SQLException e) {
            logger.error("create timeseries error", e);
        }
    }

    private TaskExecuteResult executeInsertTask(Insert insert) {
        DataView dataView = insert.getData();
        Exception e = null;
        switch (dataView.getRawDataType()) {
            case Row:
                e = insertRowRecords((RowDataView) dataView/*, storageUnit*/);
                break;
            case Column:
                e = insertColumnRecords((ColumnDataView) dataView/*, storageUnit*/);
                break;
            case NonAlignedRow:
                e = insertNonAlignedRowRecords((RowDataView) dataView/*, storageUnit*/);
                break;
            case NonAlignedColumn:
                e = insertNonAlignedColumnRecords((ColumnDataView) dataView/*, storageUnit*/);
                break;
        }
        if (e != null) {
            return new TaskExecuteResult(null, new PhysicalException("execute insert task in postgresql failure", e));
        }
        return new TaskExecuteResult(null, null);
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

    private Exception insertNonAlignedRowRecords(RowDataView data) {
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
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
        try {
            Statement stmt = connection.createStatement();
            for (int i = 0; i < data.getPathNum(); i++) {
                String path = data.getPath(i);
                DataType dataType = data.getDataType(i);
                String table = path.substring(0, path.lastIndexOf('.'));
                table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String field = path.substring(path.lastIndexOf('.') + 1);
                field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                Map<String, String> tags = data.getTags(i);
                createTimeSeriesIfNotExists(table, field, tags, dataType);
                BitmapView bitmapView = data.getBitmapView(i);
                int index = 0;
                for (int j = 0; j < data.getTimeSize(); j++) {
                    if (bitmapView.get(j)) {
                        long time = data.getTimestamp(j) / 1000; // timescaledb存10位时间戳，java为13位时间戳
                        String value;
                        if (data.getDataType(i) == DataType.BINARY) {
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
            return e;
        }

        return null;
    }
    private Exception insertNonAlignedColumnRecords(ColumnDataView data) {
        int batchSize = Math.min(data.getTimeSize(), BATCH_SIZE);
        try {
            Statement stmt = connection.createStatement();
            for (int i = 0; i < data.getPathNum(); i++) {
                String path = data.getPath(i);
                DataType dataType = data.getDataType(i);
                String table = path.substring(0, path.lastIndexOf('.'));
                table = table.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                String field = path.substring(path.lastIndexOf('.') + 1);
                field = field.replace(IGINX_SEPARATOR, POSTGRESQL_SEPARATOR);
                Map<String, String> tags = data.getTags(i);
                createTimeSeriesIfNotExists(table, field, tags, dataType);
                BitmapView bitmapView = data.getBitmapView(i);
                int index = 0;
                for (int j = 0; j < data.getTimeSize(); j++) {
                    if (bitmapView.get(j)) {
                        long time = data.getTimestamp(j) / 1000; // timescaledb存10位时间戳，java为13位时间戳
                        String value;
                        if (data.getDataType(i) == DataType.BINARY) {
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
            return e;
        }

        return null;
    }

    private TaskExecuteResult executeDeleteTask(Delete delete) {
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
                    new PhysicalTaskExecuteFailureException("execute delete task in postgresql failure",
                            e));
        }
    }
}