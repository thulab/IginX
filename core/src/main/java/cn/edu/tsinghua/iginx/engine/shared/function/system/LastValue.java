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
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LastValue implements SetMappingFunction {

    public static final String LAST_VALUE = "last_value";

    private static final LastValue INSTANCE = new LastValue();

    private LastValue() {}

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
        return LAST_VALUE;
    }

    @Override
    public Row transform(RowStream rows, List<Value> params) throws Exception {
        if (params.size() != 1) {
            throw new IllegalArgumentException("unexpected params for last value.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for last value.");
        }
        String target = param.getBinaryVAsString();
        if (target.endsWith(Constants.ALL_PATH)) {
            List<Field> fields = rows.getHeader().getFields();
            List<Field> targetFields = new ArrayList<>();
            for (Field field: fields) {
                targetFields.add(new Field(getIdentifier() + "(" + field.getName() + ")", field.getType()));
            }
            Object[] targetValues = new Object[targetFields.size()];
            while (rows.hasNext()) {
                Row row = rows.next();
                for (int i = 0; i < fields.size(); i++) {
                    Object value = row.getValue(i);
                    if (value == null) {
                        continue;
                    }
                    targetValues[i] = value;
                }
            }
            return new Row(new Header(targetFields), targetValues);
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) {
                return Row.EMPTY_ROW;
            }
            Field field = rows.getHeader().getField(index);
            Field targetField = new Field(getIdentifier() + "(" + field.getName() + ")", field.getType());
            Object targetValue = null;
            while (rows.hasNext()) {
                Row row = rows.next();
                Object value = row.getValue(index);
                if (value != null) {
                    targetValue = value;
                }
            }
            return new Row(new Header(Collections.singletonList(targetField)), new Object[]{targetValue});
        }
    }

    public static LastValue getInstance() {
        return INSTANCE;
    }

}
