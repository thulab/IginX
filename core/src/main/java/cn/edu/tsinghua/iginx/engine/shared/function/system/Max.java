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
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class Max implements SetMappingFunction {

  public static final String MAX = "max";

  private static final Max INSTANCE = new Max();

  private Max() {
  }

  public static Max getInstance() {
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
    return MAX;
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
    if (StringUtils.isPattern(target)) {
      Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
      List<Field> targetFields = new ArrayList<>();
      List<Integer> indices = new ArrayList<>();
      for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
        Field field = rows.getHeader().getField(i);
        if (pattern.matcher(field.getName()).matches()) {
          targetFields
              .add(new Field(getIdentifier() + "(" + field.getName() + ")", field.getType()));
          indices.add(i);
        }
      }
      Object[] targetValues = new Object[targetFields.size()];
      while (rows.hasNext()) {
        Row row = rows.next();
        Object[] values = row.getValues();
        for (int i = 0; i < indices.size(); i++) {
          Object value = values[indices.get(i)];
          if (targetValues[i] == null) {
            targetValues[i] = value;
          } else {
            if (value != null
                && ValueUtils.compare(targetValues[i], value, targetFields.get(i).getType()) < 0) {
              targetValues[i] = value;
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
      Field targetField = new Field(getIdentifier() + "(" + target + ")",
          rows.getHeader().getField(index).getType());
      Object targetValue = null;
      while (rows.hasNext()) {
        Row row = rows.next();
        Object value = row.getValue(index);
        if (value != null) {
          if (targetValue == null
              || ValueUtils.compare(targetValue, value, targetField.getType()) < 0) {
            targetValue = value;
          }
        }
      }
      return new Row(new Header(Collections.singletonList(targetField)), new Object[]{targetValue});
    }
  }

}
