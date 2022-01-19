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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        if (params.size() != 1) {
            throw new IllegalArgumentException("unexpected params for avg.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for avg.");
        }
        String target = param.getBinaryVAsString();
        if (target.endsWith(Constants.ALL_PATH)) {
            List<Field> targetFields = new ArrayList<>();
            for (Field field : rows.getHeader().getFields()) {
                targetFields.add(new Field(getIdentifier() + "(" + field.getName() + ")", DataType.LONG));
            }
            long[] counts = new long[targetFields.size()];
            while(rows.hasNext()) {
                Row row = rows.next();
                Object[] values = row.getValues();
                for (int i = 0; i < targetFields.size(); i++) {
                    if (values[i] != null) {
                        counts[i]++;
                    }
                }
            }
            Object[] targetValues = new Object[targetFields.size()];
            for (int i = 0; i < counts.length; i++) {
                targetValues[i] = counts[i];
            }
            return new Row(new Header(targetFields), targetValues);
        } else {
            Header header = new Header(Collections.singletonList(new Field(getIdentifier() + "(" + target + ")", DataType.LONG)));
            long count = 0L;
            int index = rows.getHeader().indexOf(target);
            if (index != -1) {
                while(rows.hasNext()) {
                    Row row = rows.next();
                    Object value = row.getValue(index);
                    if (value != null) {
                        count++;
                    }
                }
            }
            return new Row(header, new Object[]{count});
        }
    }

}
