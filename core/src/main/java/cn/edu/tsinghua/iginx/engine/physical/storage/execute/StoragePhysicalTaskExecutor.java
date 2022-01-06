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
package cn.edu.tsinghua.iginx.engine.physical.storage.execute;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.MemoryPhysicalTaskDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.queue.StoragePhysicalTaskQueue;
import cn.edu.tsinghua.iginx.engine.physical.task.MemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitHook;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class StoragePhysicalTaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(StoragePhysicalTaskExecutor.class);

    private static final StoragePhysicalTaskExecutor INSTANCE = new StoragePhysicalTaskExecutor();

    private final IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final StorageManager storageManager = new StorageManager(metaManager.getStorageEngineList());

    private final Map<String, StoragePhysicalTaskQueue> storageTaskQueues = new ConcurrentHashMap<>();

    private final Map<String, ExecutorService> dispatchers = new ConcurrentHashMap<>();

    private MemoryPhysicalTaskDispatcher memoryTaskExecutor;

    private StoragePhysicalTaskExecutor() {
        StorageUnitHook storageUnitHook = (before, after) -> {
            if (before == null && after != null) { // 新增加 du，处理这种事件，其他事件暂时不处理
                logger.info("new storage unit " + after.getId() + " come!");
                String id = after.getId();
                storageTaskQueues.put(id, new StoragePhysicalTaskQueue());
                // 为拥有该分片的存储创建一个调度线程，用于调度任务执行
                ExecutorService dispatcher = Executors.newSingleThreadExecutor();
                long storageId = after.getStorageEngineId();
                dispatchers.put(id, dispatcher);
                dispatcher.submit(() -> {
                    StoragePhysicalTaskQueue taskQueue = storageTaskQueues.get(id);
                    Pair<IStorage, ExecutorService> pair = storageManager.getStorage(storageId);
                    while(true) {
                        StoragePhysicalTask task = taskQueue.getTask();
                        task.setStorageUnit(id);
                        logger.info("take out new task: " + task);
                        pair.v.submit(() -> {
                            TaskExecuteResult result = null;
                            try {
                                result = pair.k.execute(task);
                                logger.info("task " + task + " execute finished");
                            } catch (Exception e) {
                                logger.error("execute task error: " + e);
                                result = new TaskExecuteResult(new PhysicalException(e));
                            }
                            task.setResult(result);
                            if (task.getFollowerTask() != null && task.isSync()) { // 只有同步任务才会影响后续任务的执行
                                MemoryPhysicalTask followerTask = (MemoryPhysicalTask) task.getFollowerTask();
                                boolean isFollowerTaskReady = followerTask.notifyParentReady();
                                if (isFollowerTaskReady) {
                                    memoryTaskExecutor.addMemoryTask(followerTask);
                                }
                            }
                            if (task.isNeedBroadcasting()) { // 需要传播
                                List<String> replicaIds = task.getTargetFragment().getMasterStorageUnit().getReplicas()
                                        .stream().map(StorageUnitMeta::getId).collect(Collectors.toList());
                                for (String replicaId: replicaIds) {
                                    StoragePhysicalTask replicaTask = new StoragePhysicalTask(task.getOperators(), false, false);
                                    storageTaskQueues.get(replicaId).addTask(replicaTask);
                                    logger.info("broadcasting task " + task + " to " + replicaId);
                                }
                            }
                        });
                    }
                });
            }
        };
        StorageEngineChangeHook storageEngineChangeHook = (before, after) -> {
            if (before == null && after != null) { // 新增加存储，处理这种事件，其他事件暂时不处理
                if (after.getCreatedBy() != metaManager.getIginxId()) {
                    storageManager.addStorage(after);
                }
            }
        };
        metaManager.registerStorageEngineChangeHook(storageEngineChangeHook);
        metaManager.registerStorageUnitHook(storageUnitHook);
    }

    public static StoragePhysicalTaskExecutor getInstance() {
        return INSTANCE;
    }

    public void commit(StoragePhysicalTask task) {
        commit(Collections.singletonList(task));
    }

    public void commit(List<StoragePhysicalTask> tasks) {
        for (StoragePhysicalTask task : tasks) {
            storageTaskQueues.get(task.getTargetFragment().getMasterStorageUnitId()).addTask(task); // 异步写备，查询只查主
        }
    }

    public void init(MemoryPhysicalTaskDispatcher memoryTaskExecutor) {
        this.memoryTaskExecutor = memoryTaskExecutor;
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }
}
