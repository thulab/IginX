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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive.Table;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Avg implements SetMappingFunction {

    public static final String AVG = "avg";

    private static final Avg INSTANCE = new Avg();

    private Avg() {}

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
        return AVG;
    }

    @Override
    public Row transform(RowStream rows, List<Value> params) throws Exception {
        if (params.size() != 1) {
            throw new IllegalArgumentException("unexpected params for avg.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for avg.");
        }
        String target = param.getBinaryVAsString();
        if (target.endsWith(Constants.ALL_PATH)) {
            List<Field> fields = rows.getHeader().getFields();
            for (Field field: fields) {
                if (!DataTypeUtils.isNumber(field.getType())) {
                    throw new IllegalArgumentException("only number can calculate average");
                }
            }
            double[] targetSums = new double[fields.size()];
            long[] counts = new long[fields.size()];

            while (rows.hasNext()) {
                Row row = rows.next();
                for (int i = 0; i < fields.size(); i++) {
                    Object value = row.getValue(i);
                    if (value == null) {
                        continue;
                    }
                    switch (fields.get(i).getType()) {
                        case INTEGER:
                            targetSums[i] += (int) value;
                            break;
                        case LONG:
                            targetSums[i] += (long) value;
                            break;
                        case FLOAT:
                            targetSums[i] += (float) value;
                            break;
                        case DOUBLE:
                            targetSums[i] += (double) value;
                            break;
                    }
                    counts[i]++;
                }
            }
            List<Field> targetFields = new ArrayList<>();
            for (Field field: fields) {
                targetFields.add(new Field(getIdentifier() + "(" + field.getName() + ")", DataType.DOUBLE));
            }
            Object[] targetValues = new Object[targetFields.size()];
            for (int i = 0; i < targetValues.length; i++) {
                targetValues[i] = targetSums[i] / counts[i];
            }
            return new Row(new Header(targetFields), targetValues);
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) { // 实际上没有该列
                return Row.EMPTY_ROW;
            }
            Field field = rows.getHeader().getField(index);
            if (!DataTypeUtils.isNumber(field.getType())) {
                throw new IllegalArgumentException("only number can calculate average");
            }
            double targetSum = 0.0D;
            long count = 0;
            while (rows.hasNext()) {
                Row row = rows.next();
                Object value = row.getValue(index);
                if (value == null) {
                    continue;
                }
                switch (field.getType()) {
                    case INTEGER:
                        targetSum += (int) value;
                        break;
                    case LONG:
                        targetSum += (long) value;
                        break;
                    case FLOAT:
                        targetSum += (float) value;
                        break;
                    case DOUBLE:
                        targetSum += (double) value;
                        break;
                }
                count++;
            }
            Field targetField = new Field(getIdentifier() + "(" + field.getName() + ")", DataType.DOUBLE);
            double targetValue = targetSum / count;
            return new Row(new Header(Collections.singletonList(targetField)), new Object[]{targetValue});
        }
    }


    public static Avg getInstance() {
        return INSTANCE;
    }

}
