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
package cn.edu.tsinghua.iginx.engine.shared.function.system.utils;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class ValueUtils {

    private static final Set<DataType> numericTypeSet = new HashSet<>(Arrays.asList(
        DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE));

    public static boolean isNumericType(Value value) {
        return numericTypeSet.contains(value.getDataType());
    }

    public static Value transformToDouble(Value value) {
        DataType dataType = value.getDataType();
        double dVal;
        switch (dataType) {
            case INTEGER:
                dVal = value.getIntV().doubleValue();
                break;
            case LONG:
                dVal = value.getLongV().doubleValue();
                break;
            case FLOAT:
                dVal = value.getFloatV().doubleValue();
                break;
            case DOUBLE:
                dVal = value.getDoubleV();
                break;
            case BOOLEAN:
                dVal = value.getBoolV() ? 1.0D : 0.0D;
                break;
            case BINARY:
                dVal = Double.parseDouble(value.getBinaryVAsString());
                break;
            default:
                throw new IllegalArgumentException("Unexpected dataType: " + dataType);
        }
        return new Value(DataType.DOUBLE, dVal);
    }

    public static int compare(Value o1, Value o2) {
        DataType dataType = o1.getDataType();
        switch (dataType) {
            case INTEGER:
                return Integer.compare(o1.getIntV(), o2.getIntV());
            case LONG:
                return Long.compare(o1.getLongV(), o2.getLongV());
            case BOOLEAN:
                return Boolean.compare(o1.getBoolV(), o2.getBoolV());
            case FLOAT:
                return Float.compare(o1.getFloatV(), o2.getFloatV());
            case DOUBLE:
                return Double.compare(o1.getDoubleV(), o2.getDoubleV());
            case BINARY:
                return o1.getBinaryVAsString().compareTo(o2.getBinaryVAsString());
        }
        return 0;
    }

    public static int compare(Object o1, Object o2, DataType dataType) {
        switch (dataType) {
            case INTEGER:
                return Integer.compare((Integer) o1, (Integer) o2);
            case LONG:
                return Long.compare((Long) o1, (Long) o2);
            case BOOLEAN:
                return Boolean.compare((Boolean) o1, (Boolean) o2);
            case FLOAT:
                return Float.compare((Float) o1, (Float) o2);
            case DOUBLE:
                return Double.compare((Double) o1, (Double) o2);
            case BINARY:
                return (new String((byte[]) o1)).compareTo(new String((byte[]) o2));
        }
        return 0;
    }

    public static String toString(Object value, DataType dataType) {
        switch (dataType) {
            case INTEGER:
            case LONG:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return value.toString();
            case BINARY:
                return new String((byte[]) value);
        }
        return "";
    }

}
