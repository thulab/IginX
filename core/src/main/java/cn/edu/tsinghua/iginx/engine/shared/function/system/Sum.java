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
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;

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
    public Row transform(RowStream rows, List<Value> params) {
        if (params.size() != 1) {
            throw new IllegalArgumentException("unexpected params for sum.");
        }
        Value param = params.get(0);
        if (param.getDataType() != DataType.BINARY) {
            throw new IllegalArgumentException("unexpected param type for sum.");
        }
        String target = param.getBinaryV();
        if (target.endsWith(Constants.ALL_PATH)) { // 对于任意序列均统计
            List<Field> fields = rows.getHeader().getFields();
            for (Field field: fields) {
                if (!DataTypeUtils.isNumber(field.getType())) {
                    throw new IllegalArgumentException("only number can calculate sum");
                }
            }

        } else {
            int index = rows.getHeader().indexOf(target);
            if (index == -1) { // 实际上没有该列
                return Row.EMPTY_ROW;
            }
            Field field = rows.getHeader().getField(index);
            if (!DataTypeUtils.isNumber(field.getType())) {
                throw new IllegalArgumentException("only number can calculate sum");
            }

        }
        return null;
    }

    public static Sum getInstance() {
        return INSTANCE;
    }
}
