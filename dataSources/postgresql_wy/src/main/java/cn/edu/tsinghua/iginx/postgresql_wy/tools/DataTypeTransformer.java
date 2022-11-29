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
package cn.edu.tsinghua.iginx.postgresql_wy.tools;

import cn.edu.tsinghua.iginx.thrift.DataType;

import static cn.edu.tsinghua.iginx.thrift.DataType.*;

public class DataTypeTransformer {

//    public static final String BOOLEAN = "BOOLEAN";
    public static final String FLOAT = "FLOAT";
//    public static final String DOUBLE = "DOUBLE";
    public static final String INT32 = "INT32";
    public static final String INT64 = "INT64";
    public static final String TEXT = "TEXT";

    public static DataType fromPostgreSQL(String dataType) {
        boolean b=(dataType.contains("int") || dataType.contains("timestamptz") || dataType.contains("serial"));
        if (b) {
            return LONG;
        } else if (dataType.contains("bool")) {
            return BOOLEAN;
        } else if (dataType.contains("float")) {
            return DOUBLE;
        } else {
            return BINARY;
        }
    }

    public static String toPostgreSQL(DataType dataType) {
        switch (dataType){
            case BOOLEAN:
                return "BOOLEAN";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case BINARY:
            default:
                return "TEXT";
        }
    }
}
