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
package cn.edu.tsinghua.iginx.engine.shared.function.manager;

import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Avg;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Count;
import cn.edu.tsinghua.iginx.engine.shared.function.system.First;
import cn.edu.tsinghua.iginx.engine.shared.function.system.FirstValue;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Last;
import cn.edu.tsinghua.iginx.engine.shared.function.system.LastValue;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Min;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Sum;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class FunctionManager {

    private final Map<String, Function> functions;

    private FunctionManager() {
        this.functions = new HashMap<>();
        this.initSystemFunctions();
    }

    private void initSystemFunctions() {
        registerFunction(Avg.getInstance());
        registerFunction(Count.getInstance());
        registerFunction(FirstValue.getInstance());
        registerFunction(LastValue.getInstance());
        registerFunction(First.getInstance());
        registerFunction(Last.getInstance());
        registerFunction(Max.getInstance());
        registerFunction(Min.getInstance());
        registerFunction(Sum.getInstance());
    }

    public void registerFunction(Function function) {
        if (functions.containsKey(function.getIdentifier())) {
            return;
        }
        functions.put(function.getIdentifier(), function);
    }

    public static FunctionManager getInstance() {
        return FunctionManagerHolder.INSTANCE;
    }

    public Collection<Function> getFunctions() {
        return functions.values();
    }

    public Function getFunction(String identifier) {
        return functions.get(identifier);
    }

    public boolean hasFunction(String identifier) {
        return functions.containsKey(identifier);
    }

    private static class FunctionManagerHolder {

        private static final FunctionManager INSTANCE = new FunctionManager();

        private FunctionManagerHolder() {}

    }

}
