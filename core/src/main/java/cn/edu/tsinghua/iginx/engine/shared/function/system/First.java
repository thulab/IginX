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
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public class First implements MappingFunction {

    public static final String FIRST = "first";

    private static final First INSTANCE = new First();

    private static final String PATH = "path";

    private static final String VALUE = "value";

    private First() {
    }

    public static First getInstance() {
        return INSTANCE;
    }

    @Override
    public FunctionType getFunctionType() {
        return FunctionType.System;
    }

    @Override
    public MappingType getMappingType() {
        return MappingType.Mapping;
    }

    @Override
    public String getIdentifier() {
        return FIRST;
    }

    @Override
    public RowStream transform(RowStream rows, List<Value> params) throws Exception {
        if (params.size() != 1) {
            throw new IllegalArgumentException("unexpected params for first.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for first.");
        }
        String target = param.getBinaryVAsString();
        Header header = new Header(Field.TIME, Arrays.asList(new Field(PATH, DataType.BINARY), new Field(VALUE, DataType.BINARY)));
        List<Row> resultRows = new ArrayList<>();
        if (StringUtils.isPattern(target)) {
            Map<Integer, Pair<Long, Object>> valueMap = new HashMap<>();
            Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
            Set<Integer> indices = new HashSet<>();
            for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
                Field field = rows.getHeader().getField(i);
                if (pattern.matcher(field.getName()).matches()) {
                    indices.add(i);
                }
            }
            while (rows.hasNext() && valueMap.size() < indices.size()) {
                Row row = rows.next();
                Object[] values = row.getValues();

                for (int i = 0; i < values.length; i++) {
                    if (values[i] == null || !indices.contains(i)) {
                        continue;
                    }
                    if (!valueMap.containsKey(i)) {
                        valueMap.put(i, new Pair<>(row.getTimestamp(), values[i]));
                    }
                }
            }
            for (Map.Entry<Integer, Pair<Long, Object>> entry : valueMap.entrySet()) {
                resultRows.add(new Row(header, entry.getValue().k, new Object[]{rows.getHeader().getField(entry.getKey()).getName().getBytes(StandardCharsets.UTF_8),
                    ValueUtils.toString(entry.getValue().v, rows.getHeader().getField(entry.getKey()).getType()).getBytes(StandardCharsets.UTF_8)}));
            }
            resultRows.sort(Comparator.comparingLong(Row::getTimestamp));
        } else {
            int index = rows.getHeader().indexOf(target);
            if (index != -1) {
                // 处理某一列的第一个值
                long timestamp = 0L;
                String value = null;
                while (rows.hasNext()) {
                    Row row = rows.next();
                    if (row.getValue(index) != null) {
                        timestamp = row.getTimestamp();
                        value = ValueUtils.toString(row.getValue(index), row.getType(index));
                        break;
                    }
                }
                if (value != null) {
                    resultRows.add(new Row(header, timestamp, new Object[]{rows.getHeader().getField(index).getName().getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)}));
                }
            }
        }
        return new Table(header, resultRows);
    }

}
