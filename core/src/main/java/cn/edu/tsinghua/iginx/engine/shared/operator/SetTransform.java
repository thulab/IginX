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

import cn.edu.tsinghua.iginx.engine.shared.data.Source;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;

public class SetTransform extends AbstractUnaryOperator {

    private final Function function;

    public SetTransform(Source source, Function function) {
        super(OperatorType.SetTransform, source);
        if (function == null) {
            throw new IllegalArgumentException("function shouldn't be null");
        }
        if (function.getMappingType() != MappingType.SetMapping) {
            throw new IllegalArgumentException("function should be set mapping function");
        }
        this.function = function;
    }

    public Function getFunction() {
        return function;
    }
}
