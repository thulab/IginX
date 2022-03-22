package cn.edu.tsinghua.iginx.opentsdb.tools;

import cn.edu.tsinghua.iginx.exceptions.UnsupportedDataTypeException;
import cn.edu.tsinghua.iginx.thrift.DataType;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;

public class DataTypeTransformer {

    public static final String DATA_TYPE = "data_type";

    public static DataType fromOpenTSDB(String dataType) {
        switch (dataType.toLowerCase()) {
            case "boolean":
                return BOOLEAN;
            case "integer":
                return INTEGER;
            case "long":
                return LONG;
            case "float":
                return FLOAT;
            case "double":
                return DOUBLE;
            case "binary":
                return BINARY;
            default:
                throw new UnsupportedDataTypeException(dataType);
        }
    }

    public static Object getValue(String dataType, Number data) {
        switch (dataType.toLowerCase()) {
            case "boolean":
                return data.intValue() == 1;
            case "integer":
                return data.intValue();
            case "long":
                return data.longValue();
            case "float":
                return data.floatValue();
            case "double":
                return data.doubleValue();
            default:
                throw new UnsupportedDataTypeException(dataType);
        }
    }
}
