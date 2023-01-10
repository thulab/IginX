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
package cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Timeseries;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.List;

public class IStorageWrapper implements IStorage {

    private static final String ERROR_MESSAGE = "storage is blocked due to loss connection";

    private final IStorage storage;

    private volatile boolean blocked;

    public IStorageWrapper(IStorage storage) {
        this(storage, false);
    }

    public IStorageWrapper(IStorage storage, boolean blocked) {
        this.storage = storage;
        this.blocked = blocked;
    }

    @Override
    public Connector getConnector() {
        return storage.getConnector();
    }

    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) {
        if (blocked) {
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException(ERROR_MESSAGE));
        }
        return storage.execute(task);
    }

    @Override
    public List<Timeseries> getTimeSeries() throws PhysicalException {
        if (blocked) {
            throw new PhysicalTaskExecuteFailureException(ERROR_MESSAGE);
        }
        return storage.getTimeSeries();
    }

    @Override
    public Pair<TimeSeriesRange, TimeInterval> getBoundaryOfStorage(String prefix) throws PhysicalException {
        if (blocked) {
            throw new PhysicalTaskExecuteFailureException(ERROR_MESSAGE);
        }
        return storage.getBoundaryOfStorage(prefix);
    }

    @Override
    public void release() throws PhysicalException {
        if (blocked) {
            throw new PhysicalTaskExecuteFailureException(ERROR_MESSAGE);
        }
        storage.release();
    }

    public void setBlocked(boolean blocked) {
        this.blocked = blocked;
    }

    public boolean isBlocked() {
        return blocked;
    }
}
