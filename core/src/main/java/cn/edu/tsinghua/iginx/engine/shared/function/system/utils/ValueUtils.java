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

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;


public class ValueUtils {

    private static final Set<DataType> numericTypeSet = new HashSet<>(Arrays.asList(
        DataType.INTEGER, DataType.LONG, DataType.FLOAT, DataType.DOUBLE));

    public static boolean isNumericType(Value value) {
        return numericTypeSet.contains(value.getDataType());
    }
    
    public static boolean isNumericType(DataType dataType) {
        return numericTypeSet.contains(dataType);
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

    public static int compare(Value v1, Value v2) throws PhysicalException {
        DataType dataType1 =  v1.getDataType();
        DataType dataType2 =  v2.getDataType();
        if (dataType1 != dataType2) {
            if (numericTypeSet.contains(dataType1) && numericTypeSet.contains(dataType2)) {
                v1 = transformToDouble(v1);
                v2 = transformToDouble(v2);
            } else {
                throw new InvalidOperatorParameterException(dataType1.toString() + " and " + dataType2.toString() + " can't be compared");
            }
        }
        switch (dataType1) {
            case INTEGER:
                return Integer.compare(v1.getIntV(), v2.getIntV());
            case LONG:
                return Long.compare(v1.getLongV(), v2.getLongV());
            case BOOLEAN:
                return Boolean.compare(v1.getBoolV(), v2.getBoolV());
            case FLOAT:
                return Float.compare(v1.getFloatV(), v2.getFloatV());
            case DOUBLE:
                return Double.compare(v1.getDoubleV(), v2.getDoubleV());
            case BINARY:
                return v1.getBinaryVAsString().compareTo(v2.getBinaryVAsString());
        }
        return 0;
    }

    public static boolean regexCompare(Value value, Value regex) {
        if (!value.getDataType().equals(DataType.BINARY) || !regex.getDataType().equals(DataType.BINARY)) {
            // regex can only be compared between strings.
            return false;
        }

        String valueStr = value.getBinaryVAsString();
        String regexStr = regex.getBinaryVAsString();

        return Pattern.matches(regexStr, valueStr);
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
    
    public static int compare(Object o1, Object o2, DataType dataType1, DataType dataType2) throws PhysicalException {
        if (dataType1 != dataType2) {
            if (numericTypeSet.contains(dataType1) && numericTypeSet.contains(dataType2)) {
                Value v1 = ValueUtils.transformToDouble(new Value(dataType1, o1));
                Value v2 = ValueUtils.transformToDouble(new Value(dataType2, o2));
                return compare(v1, v2);
            } else {
                throw new InvalidOperatorParameterException(dataType1.toString() + " and " + dataType2.toString() + " can't be compared");
            }
        } else {
            return compare(o1, o2, dataType1);
        }
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
