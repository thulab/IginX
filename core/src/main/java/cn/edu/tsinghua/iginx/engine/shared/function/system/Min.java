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
package cn.edu.tsinghua.iginx.engine.shared.function.system;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Min implements SetMappingFunction {

    public static final String MIN = "min";

    private static final Min INSTANCE = new Min();

    private Min() {}

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.System;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.SetMapping;
    }

    @Override
    public String getIdentifier() {
        return MIN;
    }

    @Override
    public Row transform(RowStream rows, List<Value> params) throws Exception {
        if (params.size() != 1) {
            throw new IllegalArgumentException("unexpected params for max.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for max.");
        }
        String target = param.getBinaryVAsString();
        if (target.endsWith(Constants.ALL_PATH)) {
            List<Field> targetFields = new ArrayList<>();
            for (Field field: rows.getHeader().getFields()) {
                targetFields.add(new Field(getIdentifier() + "(" + field.getName() + ")", field.getType()));
            }
            Object[] targetValues = new Object[targetFields.size()];
            while (rows.hasNext()) {
                Row row = rows.next();
                Object[] values = row.getValues();
                for (int i = 0; i < targetFields.size(); i++) {
                    if (targetValues[i] == null) {
                        targetValues[i] = values[i];
                    } else {
                        if (values[i] != null && ValueUtils.compare(targetValues[i], values[i], targetFields.get(i).getType()) > 0) {
                            targetValues[i] = values[i];
                        }
                    }
                }
            }
            return new Row(new Header(targetFields), targetValues);
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) {
                return Row.EMPTY_ROW;
            }
            Field targetField = new Field(getIdentifier() + "(" + target + ")", rows.getHeader().getField(index).getType());
            Object targetValue = null;
            while (rows.hasNext()) {
                Row row = rows.next();
                Object value = row.getValue(index);
                if (value != null) {
                    if (targetValue == null || ValueUtils.compare(targetValue, value, targetField.getType()) > 0) {
                        targetValue = value;
                    }
                }
            }
            return new Row(new Header(Collections.singletonList(targetField)), new Object[]{targetValue});
        }
    }

    public static Min getInstance() {
        return INSTANCE;
    }

}
