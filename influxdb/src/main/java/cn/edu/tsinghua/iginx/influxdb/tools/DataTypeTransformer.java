package cn.edu.tsinghua.iginx.influxdb.tools;

import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;
import static cn.edu.tsinghua.iginx.thrift.DataType.BOOLEAN;
import static cn.edu.tsinghua.iginx.thrift.DataType.DOUBLE;
import static cn.edu.tsinghua.iginx.thrift.DataType.LONG;

import cn.edu.tsinghua.iginx.exceptions.UnsupportedDataTypeException;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class DataTypeTransformer {

  public static DataType fromInfluxDB(String dataType) {
    switch (dataType) {
      case "boolean":
        return BOOLEAN;
      case "long":
        return LONG;
      case "double":
        return DOUBLE;
      case "string":
        return BINARY;
      default:
        throw new UnsupportedDataTypeException(dataType);
    }
  }
}
