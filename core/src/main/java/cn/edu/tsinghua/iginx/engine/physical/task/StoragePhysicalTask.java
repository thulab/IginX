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
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

import java.util.List;

public class StoragePhysicalTask extends AbstractPhysicalTask {

    private final FragmentMeta targetFragment;
    private final boolean sync;
    private final boolean needBroadcasting;
    private String storageUnit;
    private long storage;

    public StoragePhysicalTask(List<Operator> operators) {
        this(operators, ((FragmentSource) ((UnaryOperator) operators.get(0)).getSource()).getFragment(), true, false);
    }

    public StoragePhysicalTask(List<Operator> operators, boolean sync, boolean needBroadcasting) {
        this(operators, ((FragmentSource) ((UnaryOperator) operators.get(0)).getSource()).getFragment(), sync, needBroadcasting);
    }

    public StoragePhysicalTask(List<Operator> operators, FragmentMeta targetFragment, boolean sync, boolean needBroadcasting) {
        super(TaskType.Storage, operators);
        this.targetFragment = targetFragment;
        this.sync = sync;
        this.needBroadcasting = needBroadcasting;
    }

    public FragmentMeta getTargetFragment() {
        return targetFragment;
    }

    public String getStorageUnit() {
        return storageUnit;
    }

    public void setStorageUnit(String storageUnit) {
        this.storageUnit = storageUnit;
    }

    public long getStorage() {
        return storage;
    }

    public void setStorage(long storage) {
        this.storage = storage;
    }

    public boolean isSync() {
        return sync;
    }

    public boolean isNeedBroadcasting() {
        return needBroadcasting;
    }

    @Override
    public String toString() {
        return "StoragePhysicalTask{" +
                "targetFragment=" + targetFragment +
                ", storageUnit='" + storageUnit + '\'' +
                ", storage=" + storage +
                '}';
    }
}
