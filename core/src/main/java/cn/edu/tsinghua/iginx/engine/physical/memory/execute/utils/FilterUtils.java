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
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.NotFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.TimeFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;

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
        if (value.getDataType() != targetValue.getDataType()) { // 类型不同，直接否了
            return false;
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
        }
        return false;
    }


}
