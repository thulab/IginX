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
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Sum implements SetMappingFunction {

    public static final String SUM = "sum";

    private static final Sum INSTANCE = new Sum();

    private Sum() {}

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
        return SUM;
    }

    @Override
    public Row transform(RowStream rows, List<Value> params) throws Exception {
        if (params.size() != 1) {
            throw new IllegalArgumentException("unexpected params for sum.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for sum.");
        }
        String target = param.getBinaryVAsString();
        if (target.endsWith(Constants.ALL_PATH)) { // 对于任意序列均统计
            List<Field> fields = rows.getHeader().getFields();
            for (Field field: fields) {
                if (!DataTypeUtils.isNumber(field.getType())) {
                    throw new IllegalArgumentException("only number can calculate sum");
                }
            }
            List<Field> targetFields = new ArrayList<>();
            for (Field field: fields) {
                targetFields.add(new Field(getIdentifier() + "(" + field.getName() + ")", field.getType()));
            }
            Object[] targetValues = new Object[targetFields.size()];
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                if (DataTypeUtils.isWholeNumber(field.getType())) {
                    targetFields.add(new Field(getIdentifier() + "(" + field.getName() + ")", DataType.LONG));
                    targetValues[i] = 0L;
                } else {
                    targetFields.add(new Field(getIdentifier() + "(" + field.getName() + ")", DataType.DOUBLE));
                    targetValues[i] = 0.0D;
                }
            }
            while (rows.hasNext()) {
                Row row = rows.next();
                for (int i = 0; i < fields.size(); i++) {
                    Object value = row.getValue(i);
                    if (value == null) {
                        continue;
                    }
                    switch (fields.get(i).getType()) {
                        case INTEGER:
                            targetValues[i] = ((long) targetValues[i]) + (int) value;
                            break;
                        case LONG:
                            targetValues[i] = ((long) targetValues[i]) + (long) value;
                            break;
                        case FLOAT:
                            targetValues[i] = ((double) targetValues[i]) + (float) value;
                            break;
                        case DOUBLE:
                            targetValues[i] = ((double) targetValues[i]) + (double) value;
                            break;
                    }
                }
            }
            return new Row(new Header(targetFields), targetValues);
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) { // 实际上没有该列
                return Row.EMPTY_ROW;
            }
            Field field = rows.getHeader().getField(index);
            if (!DataTypeUtils.isNumber(field.getType())) {
                throw new IllegalArgumentException("only number can calculate sum");
            }
            Field targetField;
            Object targetValue;
            if (DataTypeUtils.isWholeNumber(field.getType())) {
                targetField = new Field(getIdentifier() + "(" + field.getName() + ")", DataType.LONG);
                targetValue = 0L;
            } else {
                targetField = new Field(getIdentifier() + "(" + field.getName() + ")", DataType.DOUBLE);
                targetValue = 0.0D;
            }
            while (rows.hasNext()) {
                Row row = rows.next();
                Object value = row.getValue(index);
                if (value == null) {
                    continue;
                }
                switch (field.getType()) {
                    case INTEGER:
                        targetValue = ((long) targetValue) + (int) value;
                        break;
                    case LONG:
                        targetValue = ((long) targetValue) + (long) value;
                        break;
                    case FLOAT:
                        targetValue = ((double) targetValue) + (float) value;
                        break;
                    case DOUBLE:
                        targetValue = ((double) targetValue) + (double) value;
                        break;
                }
            }
            return new Row(new Header(Collections.singletonList(targetField)), new Object[]{targetValue});
        }
    }

    public static Sum getInstance() {
        return INSTANCE;
    }
}
