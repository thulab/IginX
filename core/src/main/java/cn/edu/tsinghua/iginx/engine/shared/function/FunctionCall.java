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
package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;

import java.util.HashMap;
import java.util.Map;

public class FunctionCall {

    private final Function function;

    private final Map<String, Value> params;

    public FunctionCall(Function function, Map<String, Value> params) {
        this.function = function;
        this.params = params;
    }

    public Function getFunction() {
        return function;
    }

    public Map<String, Value> getParams() {
        return params;
    }

    public FunctionCall copy() {
        return new FunctionCall(function, new HashMap<>(params));
    }
}
