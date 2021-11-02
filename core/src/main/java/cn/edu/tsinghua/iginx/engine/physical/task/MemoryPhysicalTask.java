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
import java.util.concurrent.atomic.AtomicInteger;

public abstract class MemoryPhysicalTask extends AbstractPhysicalTask {

    protected AtomicInteger parentReadyCount;

    public MemoryPhysicalTask(TaskType type, List<Operator> operators) {
        super(type, operators);
    }

    public abstract TaskExecuteResult execute(); // 在 parent 都完成执行后，可以执行该任务

    public abstract boolean notifyParentReady(); // 通知当前任务的某个父节点已经完成，该方法会返回 boolean 值，表示当前的任务是否可以执行

}
