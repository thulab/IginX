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
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class Count implements SetMappingFunction {

    public static final String COUNT = "count";
    private static final Logger logger = LoggerFactory.getLogger(Count.class);
    private static final Count INSTANCE = new Count();

    private Count() {
    }

    public static Count getInstance() {
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
        return COUNT;
    }

    @Override
    public Row transform(RowStream rows, List<Value> params) throws Exception {
        if (params.size() == 0 || params.size() > 2) {
            throw new IllegalArgumentException("unexpected params for count.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for count.");
        }
        List<Integer> groupByLevels = null;
        if (params.size() == 2) {
            groupByLevels = GroupByUtils.parseLevelsFromValue(params.get(1));
        }
        String target = param.getBinaryVAsString();
        if (StringUtils.isPattern(target)) {
            Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
            List<Field> targetFields = new ArrayList<>();
            List<Integer> indices = new ArrayList<>();
            Map<String, Integer> groupNameIndexMap = new HashMap<>(); // 只有在存在 group by 的时候才奏效
            Map<Integer, Integer> groupOrderIndexMap = new HashMap<>();
            for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
                Field field = rows.getHeader().getField(i);
                if (pattern.matcher(field.getName()).matches()) {
                    if (groupByLevels == null) {
                        targetFields.add(new Field(getIdentifier() + "(" + field.getName() + ")", DataType.LONG));
                    } else {
                        String targetFieldName = getIdentifier() + "(" + GroupByUtils.transformPath(field.getName(), groupByLevels) + ")";
                        int index = groupNameIndexMap.getOrDefault(targetFieldName, -1);
                        if (index != -1) {
                            groupOrderIndexMap.put(i, index);
                        } else {
                            groupNameIndexMap.put(targetFieldName, targetFields.size());
                            groupOrderIndexMap.put(i, targetFields.size());
                            targetFields.add(new Field(targetFieldName, DataType.LONG));
                        }
                    }
                    indices.add(i);
                }
            }
            long[] counts = new long[targetFields.size()];
            while(rows.hasNext()) {
                Row row = rows.next();
                Object[] values = row.getValues();
                for (int i = 0; i < indices.size(); i++) {
                    int index = indices.get(i);
                    if (values[index] != null) {
                        int targetIndex = i;
                        if (groupByLevels != null) {
                            targetIndex = groupOrderIndexMap.get(index);
                        }
                        counts[targetIndex]++;
                    }
                }
            }
            Object[] targetValues = new Object[targetFields.size()];
            for (int i = 0; i < counts.length; i++) {
                targetValues[i] = counts[i];
            }
            return new Row(new Header(targetFields), targetValues);
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) {
                return Row.EMPTY_ROW;
            }
            Field field = rows.getHeader().getField(index);
            Field targetField;
            String targetFieldName;
            if (groupByLevels == null) {
                targetFieldName = getIdentifier() + "(" + field.getName() + ")";
            } else {
                targetFieldName = getIdentifier() + "(" + GroupByUtils.transformPath(field.getName(), groupByLevels) + ")";
            }
            targetField = new Field(targetFieldName, DataType.LONG);
            long count = 0L;
            while(rows.hasNext()) {
                Row row = rows.next();
                Object value = row.getValue(index);
                if (value != null) {
                    count++;
                }
            }
            return new Row(new Header(Collections.singletonList(targetField)), new Object[]{count});
        }
    }

}
