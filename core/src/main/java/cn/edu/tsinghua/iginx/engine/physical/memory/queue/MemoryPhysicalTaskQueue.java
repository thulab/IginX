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
package cn.edu.tsinghua.iginx.engine.physical.memory.queue;

import cn.edu.tsinghua.iginx.engine.physical.task.MemoryPhysicalTask;

public interface MemoryPhysicalTaskQueue {

    /**
     * 向任务队列中添加任务
     *
     * @param memoryTask 内存任务
     * @return 是否成功添加任务
     */
    boolean addTask(MemoryPhysicalTask memoryTask);

    /**
     * 如果当前队列中不含未执行的计划，则该方法会阻塞。
     *
     * @return 距今最久的未执行的计划
     */
    MemoryPhysicalTask getTask();

}
