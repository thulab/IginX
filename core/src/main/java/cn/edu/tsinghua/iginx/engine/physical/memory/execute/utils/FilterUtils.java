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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class FilterUtils {

    public static boolean validate(Filter filter, Row row) {
        switch (filter.getType()) {
            case Or:
                OrFilter orFilter = (OrFilter) filter;
                for (Filter childFilter : orFilter.getChildren()) {
                    if (validate(childFilter, row)) { // 任何一个子条件满足，都直接返回
                        return true;
                    }
                }
                return false;
            case Bool:
                break;
            case And:
                AndFilter andFilter = (AndFilter) filter;
                for (Filter childFilter : andFilter.getChildren()) {
                    if (!validate(childFilter, row)) { // 任何一个子条件不满足，都直接返回
                        return false;
                    }
                }
                return true;
            case Not:
                NotFilter notFilter = (NotFilter) filter;
                return !validate(notFilter.getChild(), row);
            case Time:
                TimeFilter timeFilter = (TimeFilter) filter;
                if (row.getTimestamp() == Row.NON_EXISTED_TIMESTAMP) {
                    return false;
                }
                return validateTimeFilter(timeFilter, row);
            case Value:
                ValueFilter valueFilter = (ValueFilter) filter;
                return validateValueFilter(valueFilter, row);
            case Path:
                PathFilter pathFilter = (PathFilter) filter;
                return validatePathFilter(pathFilter, row);
            default:
                break;
        }
        return false;
    }

    private static boolean validateTimeFilter(TimeFilter timeFilter, Row row) {
        long timestamp = row.getTimestamp();
        switch (timeFilter.getOp()) {
            case E:
                return timestamp == timeFilter.getValue();
            case G:
                return timestamp > timeFilter.getValue();
            case L:
                return timestamp < timeFilter.getValue();
            case GE:
                return timestamp >= timeFilter.getValue();
            case LE:
                return timestamp <= timeFilter.getValue();
            case NE:
                return timestamp != timeFilter.getValue();
        }
        return false;
    }

    private static boolean validateValueFilter(ValueFilter valueFilter, Row row) {
        Value value = row.getAsValue(valueFilter.getPath());
        Value targetValue = valueFilter.getValue();
        if (value == null || value.isNull() || targetValue.isNull()) { // 如果任何一个是空值，则认为不可比较
            return false;
        }

        if (value.getDataType() != targetValue.getDataType()) {
            if (ValueUtils.isNumericType(value) && ValueUtils.isNumericType(targetValue)) {
                value = ValueUtils.transformToDouble(value);
                targetValue = ValueUtils.transformToDouble(targetValue);
            } else {  // 数值类型和非数值类型无法比较
                return false;
            }
        }

        switch (valueFilter.getOp()) {
            case E:
                return ValueUtils.compare(value, targetValue) == 0;
            case G:
                return ValueUtils.compare(value, targetValue) > 0;
            case L:
                return ValueUtils.compare(value, targetValue) < 0;
            case GE:
                return ValueUtils.compare(value, targetValue) >= 0;
            case LE:
                return ValueUtils.compare(value, targetValue) <= 0;
            case NE:
                return ValueUtils.compare(value, targetValue) != 0;
            case LIKE:
                return ValueUtils.regexCompare(value, targetValue);
        }
        return false;
    }

    private static boolean validatePathFilter(PathFilter pathFilter, Row row) {
        Value valueA = row.getAsValue(pathFilter.getPathA());
        Value valueB = row.getAsValue(pathFilter.getPathB());
        if (valueA == null || valueA.isNull() || valueB == null || valueB.isNull()) { // 如果任何一个是空值，则认为不可比较
            return false;
        }

        if (valueA.getDataType() != valueB.getDataType()) {
            if (ValueUtils.isNumericType(valueA) && ValueUtils.isNumericType(valueB)) {
                valueA = ValueUtils.transformToDouble(valueA);
                valueB = ValueUtils.transformToDouble(valueB);
            } else {  // 数值类型和非数值类型无法比较
                return false;
            }
        }

        switch (pathFilter.getOp()) {
            case E:
                return ValueUtils.compare(valueA, valueB) == 0;
            case G:
                return ValueUtils.compare(valueA, valueB) > 0;
            case L:
                return ValueUtils.compare(valueA, valueB) < 0;
            case GE:
                return ValueUtils.compare(valueA, valueB) >= 0;
            case LE:
                return ValueUtils.compare(valueA, valueB) <= 0;
            case NE:
                return ValueUtils.compare(valueA, valueB) != 0;
        }
        return false;
    }
}
