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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class RowTransform extends AbstractUnaryOperator {

  private final FunctionCall functionCall;

  public RowTransform(Source source, FunctionCall functionCall) {
    super(OperatorType.RowTransform, source);
    if (functionCall == null || functionCall.getFunction() == null) {
      throw new IllegalArgumentException("function shouldn't be null");
    }
    if (functionCall.getFunction().getMappingType() != MappingType.RowMapping) {
      throw new IllegalArgumentException("function should be set mapping function");
    }
    this.functionCall = functionCall;
  }

  public FunctionCall getFunctionCall() {
    return functionCall;
  }

  @Override
  public Operator copy() {
    return new RowTransform(getSource().copy(), functionCall.copy());
  }
}
