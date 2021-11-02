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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MemoryPhysicalTaskQueueImpl implements MemoryPhysicalTaskQueue {

    private static final Logger logger = LoggerFactory.getLogger(MemoryPhysicalTaskQueueImpl.class);

    private final BlockingQueue<MemoryPhysicalTask> tasks = new LinkedBlockingQueue<>();

    @Override
    public boolean addTask(MemoryPhysicalTask memoryTask) {
        return tasks.add(memoryTask);
    }

    @Override
    public MemoryPhysicalTask getTask() {
        try {
            return tasks.take();
        } catch (Exception e) {
            logger.error("encounter error when get memory task: ", e);
        }
        return null;
    }
}
