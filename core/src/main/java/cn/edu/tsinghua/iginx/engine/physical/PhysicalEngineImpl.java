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
package cn.edu.tsinghua.iginx.engine.physical;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.constraint.ConstraintManagerImpl;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.MemoryPhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizerManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.memory.queue.MemoryPhysicalTaskQueue;
import cn.edu.tsinghua.iginx.engine.physical.memory.queue.MemoryPhysicalTaskQueueImpl;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class PhysicalEngineImpl implements PhysicalEngine {

    private static final PhysicalEngineImpl INSTANCE = new PhysicalEngineImpl();

    private final PhysicalOptimizer optimizer;

    private final MemoryPhysicalTaskExecutor memoryTaskExecutor;

    private final StoragePhysicalTaskExecutor storageTaskExecutor;

    private PhysicalEngineImpl() {
        optimizer = PhysicalOptimizerManager.getInstance().getOptimizer(ConfigDescriptor.getInstance().getConfig().getPhysicalOptimizer());
        memoryTaskExecutor = MemoryPhysicalTaskExecutor.getInstance();
        storageTaskExecutor = StoragePhysicalTaskExecutor.getInstance();
        storageTaskExecutor.init(memoryTaskExecutor);
        memoryTaskExecutor.startDispatcher();
    }

    @Override
    public RowStream execute(Operator root) {
        PhysicalTask task = optimizer.optimize(root);
        List<StoragePhysicalTask> storageTasks = new ArrayList<>();
        getStorageTasks(storageTasks, task);
        storageTaskExecutor.commit(storageTasks);
        TaskExecuteResult result = task.getResult();
        return result.getRowStream();
    }

    private void getStorageTasks(List<StoragePhysicalTask> tasks, PhysicalTask root) {
        if (root == null) {
            return;
        }
        if (root.getType() == TaskType.Storage) {
            tasks.add((StoragePhysicalTask) root);
        } else if (root.getType() == TaskType.BinaryMemory) {
            BinaryMemoryPhysicalTask task = (BinaryMemoryPhysicalTask) root;
            getStorageTasks(tasks, task.getParentTaskA());
            getStorageTasks(tasks, task.getParentTaskB());
        } else if (root.getType() == TaskType.UnaryMemory) {
            UnaryMemoryPhysicalTask task = (UnaryMemoryPhysicalTask) root;
            getStorageTasks(tasks, task.getParentTask());
        }
    }

    @Override
    public ConstraintManager getConstraintManager() {
        return ConstraintManagerImpl.getInstance();
    }

    public static PhysicalEngineImpl getInstance() {
        return INSTANCE;
    }
}
