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
package cn.edu.tsinghua.iginx.tools.utils;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class DataTypeInferenceUtils {

    private static boolean isBoolean(String s) {
        return s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false");
    }

    private static boolean isNumber(String s) {
        if (s == null || s.equalsIgnoreCase("nan")) {
            return false;
        }
        try {
            Double.parseDouble(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    private static boolean isLong(String s) {
        return Long.parseLong(s) > (2 << 24);
    }

    public static DataType getInferredDataType(String s) {
        if (isBoolean(s)) {
            return DataType.BOOLEAN;
        } else if (isNumber(s)) {
            if (!s.contains(".")) {
                if (isLong(s)) {
                    return DataType.LONG;
                }
                return DataType.INTEGER;
            } else {
                return DataType.DOUBLE;
            }
        } else if (s.equalsIgnoreCase("null")) {
            return null;
        } else {
            return DataType.BINARY;
        }
    }
}
