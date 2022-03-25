package cn.edu.tsinghua.iginx.jdbc;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class IginXResultMetadata implements ResultSetMetaData {

  private final List<String> columnNames;
  private final List<DataType> columnTypes;
  private final boolean hasTime;

  public IginXResultMetadata(List<String> columnNames, List<DataType> columnTypes,
      boolean hasTime) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.hasTime = hasTime;
  }

  @Override
  public int getColumnCount() {
    return columnNames == null ? 0 : columnNames.size();
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    checkColumnIndex(column);
    return columnNames.get(column - 1);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return getColumnLabel(column);
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && hasTime) {
      return Types.TIMESTAMP;
    }
    DataType columnType = columnTypes.get(column - 1);
    switch (columnType) {
      case BOOLEAN:
        return Types.BOOLEAN;
      case INTEGER:
        return Types.INTEGER;
      case LONG:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case BINARY:
        return Types.VARCHAR;
      default:
        return Types.NULL;
    }
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && hasTime) {
      return "TIMESTAMP";
    }
    DataType columnType = columnTypes.get(column - 1);
    switch (columnType) {
      case BOOLEAN:
        return "BOOLEAN";
      case INTEGER:
        return "INTEGER";
      case LONG:
        return "BIGINT";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case BINARY:
        return "VARCHAR";
      default:
        return "NULL";
    }
  }

  private void checkColumnIndex(int column) throws SQLException {
    if (columnNames == null || columnNames.isEmpty()) {
      throw new SQLException("No column exists");
    }
    if (column < 1) {
      throw new SQLException("Column Index out of range, " + column + " < 1");
    }
    if (column > columnNames.size()) {
      throw new SQLException("Column Index out of range, " + column + " > " + columnNames.size());
    }
  }

  @Override
  public boolean isAutoIncrement(int column) {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) {
    return false;
  }

  @Override
  public boolean isSearchable(int column) {
    return true;
  }

  @Override
  public boolean isCurrency(int column) {
    return false;
  }

  @Override
  public int isNullable(int column) {
    if (column == 1 && hasTime) {
      return ResultSetMetaData.columnNoNulls;
    }
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    checkColumnIndex(column);
    if (column == 1 && hasTime) {
      return false;
    }
    DataType columnType = columnTypes.get(column - 1);
    switch (columnType) {
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public String getSchemaName(int column) {
    return "";
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    checkColumnIndex(column);
    DataType columnType = columnTypes.get(column - 1);
    switch (columnType) {
      case FLOAT:
        return 5;
      case DOUBLE:
        return 9;
      default:
        return 0;
    }
  }

  @Override
  public int getScale(int column) throws SQLException {
    return getPrecision(column);
  }

  @Override
  public String getTableName(int column) {
    return "";
  }

  @Override
  public String getCatalogName(int column) {
    return "";
  }

  @Override
  public boolean isReadOnly(int column) {
    return false;
  }

  @Override
  public boolean isWritable(int column) {
    return true;
  }

  @Override
  public boolean isDefinitelyWritable(int column) {
    return false;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    checkColumnIndex(column);
    DataType columnType = columnTypes.get(column - 1);
    switch (columnType) {
      case BOOLEAN:
        return Boolean.class.getName();
      case INTEGER:
        return Integer.class.getName();
      case LONG:
        return Long.class.getName();
      case FLOAT:
        return Float.class.getName();
      case DOUBLE:
        return Double.class.getName();
      case BINARY:
        return String.class.getName();
      default:
        return "";
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }
}
