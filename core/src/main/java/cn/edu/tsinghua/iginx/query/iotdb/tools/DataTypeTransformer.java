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
package cn.edu.tsinghua.iginx.query.iotdb.tools;

import cn.edu.tsinghua.iginx.utils.DataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class DataTypeTransformer {

    public static DataType fromIoTDB(TSDataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return DataType.BOOLEAN;
            case FLOAT:
                return DataType.FLOAT;
            case DOUBLE:
                return DataType.DOUBLE;
            case INT32:
                return DataType.INT32;
            case INT64:
                return DataType.INT64;
            case TEXT:
                return DataType.TEXT;
            default:
                break;
        }
        return null;
    }




}
