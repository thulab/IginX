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
package cn.edu.tsinghua.iginx.engine.physical.storage.queue;

import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class StoragePhysicalTaskQueue {

    private static final Random random = new Random();

    private final long id = random.nextLong();

    private static final Logger logger = LoggerFactory.getLogger(StoragePhysicalTaskQueue.class);

    private final BlockingQueue<StoragePhysicalTask> tasks;

    public StoragePhysicalTaskQueue() {
        tasks = new LinkedBlockingQueue<>();
    }

    public void addTask(StoragePhysicalTask task) {
        try {
            tasks.put(task);
            logger.info("[add to queue] current task [id = " + id + "] queue size: " + tasks.size());
        } catch (InterruptedException e) {
            logger.error("add task to physical task queue error: ", e);
        }
    }

    public StoragePhysicalTask getTask() {
        try {
            logger.info("[get from queue] current task [id = " + id + "] queue size: " + tasks.size());
            return tasks.take();
        } catch (Exception e) {
            logger.error("encounter error when get memory task: ", e);
        }
        return null;
    }

    public long getId() {
        return id;
    }
}
