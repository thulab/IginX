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
package cn.edu.tsinghua.iginx.iotdb.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DataTypeTransformer {

  public static final String BOOLEAN = "BOOLEAN";
  public static final String FLOAT = "FLOAT";
  public static final String DOUBLE = "DOUBLE";
  public static final String INT32 = "INT32";
  public static final String INT64 = "INT64";
  public static final String TEXT = "TEXT";

  public static TSDataType strToIoTDB(String dataType) {
    switch (dataType) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case INT32:
        return TSDataType.INT32;
      case INT64:
        return TSDataType.INT64;
      case TEXT:
        return TSDataType.TEXT;
      default:
        break;
    }
    return null;
  }

  public static DataType strFromIoTDB(String dataType) {
    switch (dataType) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case INT32:
        return DataType.INTEGER;
      case INT64:
        return DataType.LONG;
      case TEXT:
        return DataType.BINARY;
      default:
        break;
    }
    return null;
  }

  public static TSDataType toIoTDB(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case INTEGER:
        return TSDataType.INT32;
      case LONG:
        return TSDataType.INT64;
      case BINARY:
        return TSDataType.TEXT;
      default:
        break;
    }
    return null;
  }
}
