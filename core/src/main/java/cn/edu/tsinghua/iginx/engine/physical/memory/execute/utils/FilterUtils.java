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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class FilterUtils {

    public static boolean validate(Filter filter, Row row) throws PhysicalException {
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
            case Key:
                KeyFilter keyFilter = (KeyFilter) filter;
                if (row.getKey() == Row.NON_EXISTED_KEY) {
                    return false;
                }
                return validateTimeFilter(keyFilter, row);
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

    private static boolean validateTimeFilter(KeyFilter keyFilter, Row row) {
        long timestamp = row.getKey();
        switch (keyFilter.getOp()) {
            case E:
                return timestamp == keyFilter.getValue();
            case G:
                return timestamp > keyFilter.getValue();
            case L:
                return timestamp < keyFilter.getValue();
            case GE:
                return timestamp >= keyFilter.getValue();
            case LE:
                return timestamp <= keyFilter.getValue();
            case NE:
                return timestamp != keyFilter.getValue();
        }
        return false;
    }

    private static boolean validateValueFilter(ValueFilter valueFilter, Row row) throws PhysicalException {
        String path = valueFilter.getPath();
        Value targetValue = valueFilter.getValue();
        if (targetValue.isNull()) { // targetValue是空值，则认为不可比较
            return false;
        }

        if (path.contains("*")) {
            List<Value> valueList = row.getAsValueByPattern(path);
            for (Value value : valueList) {
                if (value == null || value.isNull()) { // 任何一个value是空值，则认为不可比较
                    return false;
                }
                if (!validateValueCompare(valueFilter.getOp(), value, targetValue)) { // 任何一个子条件不满足，都直接返回
                    return false;
                }
            }
            return true;
        } else {
            Value value = row.getAsValue(path);
            if (value == null || value.isNull()) { // value是空值，则认为不可比较
                return false;
            }
            return validateValueCompare(valueFilter.getOp(), value, targetValue);
        }
    }

    private static boolean validatePathFilter(PathFilter pathFilter, Row row) throws PhysicalException {
        Value valueA = row.getAsValue(pathFilter.getPathA());
        Value valueB = row.getAsValue(pathFilter.getPathB());
        if (valueA == null || valueA.isNull() || valueB == null || valueB.isNull()) { // 如果任何一个是空值，则认为不可比较
            return false;
        }
        return validateValueCompare(pathFilter.getOp(), valueA, valueB);
    }

    private static boolean validateValueCompare(Op op, Value valueA, Value valueB) throws PhysicalException {
        if (valueA.getDataType() != valueB.getDataType()) {
            if (ValueUtils.isNumericType(valueA) && ValueUtils.isNumericType(valueB)) {
                valueA = ValueUtils.transformToDouble(valueA);
                valueB = ValueUtils.transformToDouble(valueB);
            } else {  // 数值类型和非数值类型无法比较
                return false;
            }
        }

        switch (op) {
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
            case LIKE:
                return ValueUtils.regexCompare(valueA, valueB);
        }
        return false;
    }
    
    public static List<Pair<String, String>> getJoinColumnsFromFilter(Filter filter) {
        List<Pair<String, String>> l = new ArrayList<>();
        switch (filter.getType()) {
            case And:
                AndFilter andFilter = (AndFilter) filter;
                for (Filter childFilter : andFilter.getChildren()) {
                    l.addAll(getJoinColumnsFromFilter(childFilter));
                }
            case Path:
                l.add(getJoinColumnFromPathFilter((PathFilter) filter));
        }
        return l;
    }
    
    public static Pair<String, String> getJoinColumnFromPathFilter(PathFilter pathFilter) {
        if (pathFilter.getOp().equals(Op.E)) {
            return new Pair<>(pathFilter.getPathA(), pathFilter.getPathB());
        }
        return null;
    }
}
