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

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.GroupByUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.*;
import java.util.regex.Pattern;

public class Avg implements SetMappingFunction {

    public static final String AVG = "avg";

    private static final Avg INSTANCE = new Avg();

    private Avg() {
    }

    public static Avg getInstance() {
        return INSTANCE;
    }

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
        if (params.size() == 0 || params.size() > 2) {
            throw new IllegalArgumentException("unexpected params for avg.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for avg.");
        }
        List<Integer> groupByLevels = null;
        if (params.size() == 2) {
            groupByLevels = GroupByUtils.parseLevelsFromValue(params.get(1));
        }
        String target = param.getBinaryVAsString();
        List<Field> fields = rows.getHeader().getFields();
        for (Field field : fields) {
            if (!DataTypeUtils.isNumber(field.getType())) {
                throw new IllegalArgumentException("only number can calculate average");
            }
        }
        Pattern pattern = Pattern.compile(StringUtils.reformatPath(target) + ".*");
        List<Field> targetFields = new ArrayList<>();
        List<Integer> indices = new ArrayList<>();
        Map<String, Integer> groupNameIndexMap = new HashMap<>(); // 只有在存在 group by 的时候才奏效
        Map<Integer, Integer> groupOrderIndexMap = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (pattern.matcher(field.getFullName()).matches()) {
                if (groupByLevels == null) {
                    targetFields.add(new Field(getIdentifier() + "(" + field.getFullName() + ")", DataType.DOUBLE));
                } else {
                    String targetFieldName = getIdentifier() + "(" + GroupByUtils.transformPath(field.getFullName(), groupByLevels) + ")";
                    int index = groupNameIndexMap.getOrDefault(targetFieldName, -1);
                    if (index != -1) {
                        groupOrderIndexMap.put(i, index);
                    } else {
                        groupNameIndexMap.put(targetFieldName, targetFields.size());
                        groupOrderIndexMap.put(i, targetFields.size());
                        targetFields.add(new Field(targetFieldName, DataType.DOUBLE));
                    }
                }
                indices.add(i);
            }
        }

        double[] targetSums = new double[targetFields.size()];
        long[] counts = new long[targetFields.size()];
        while (rows.hasNext()) {
            Row row = rows.next();
            for (int i = 0; i < indices.size(); i++) {
                int index = indices.get(i);
                Object value = row.getValue(index);
                if (value == null) {
                    continue;
                }
                int targetIndex = i;
                if (groupByLevels != null) {
                    targetIndex = groupOrderIndexMap.get(index);
                }
                switch (fields.get(index).getType()) {
                    case INTEGER:
                        targetSums[targetIndex] += (int) value;
                        break;
                    case LONG:
                        targetSums[targetIndex] += (long) value;
                        break;
                    case FLOAT:
                        targetSums[targetIndex] += (float) value;
                        break;
                    case DOUBLE:
                        targetSums[targetIndex] += (double) value;
                        break;
                }
                counts[targetIndex]++;
            }
        }
        Object[] targetValues = new Object[targetFields.size()];
        for (int i = 0; i < targetValues.length; i++) {
            targetValues[i] = targetSums[i] / counts[i];
        }
        return new Row(new Header(targetFields), targetValues);
    }

}
