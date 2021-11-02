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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

import java.util.List;

public class BinaryMemoryPhysicalTask extends MemoryPhysicalTask {

    private final PhysicalTask parentTaskA;

    private final PhysicalTask parentTaskB;

    public BinaryMemoryPhysicalTask(List<Operator> operators, PhysicalTask parentTaskA, PhysicalTask parentTaskB) {
        super(operators);
        this.parentTaskA = parentTaskA;
        this.parentTaskB = parentTaskB;
    }

    public PhysicalTask getParentTaskA() {
        return parentTaskA;
    }

    public PhysicalTask getParentTaskB() {
        return parentTaskB;
    }

    @Override
    public TaskType getType() {
        return TaskType.BinaryMemory;
    }

    @Override
    public TaskExecuteResult execute() {
        return null;
    }

    @Override
    public boolean notifyParentReady() {
        return parentReadyCount.incrementAndGet() == 2;
    }
}
