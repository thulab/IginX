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
package cn.edu.tsinghua.iginx.engine.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.engine.physical.optimizer.ReplicaDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitState;

public class NaiveReplicaDispatcher implements ReplicaDispatcher {

    private static final NaiveReplicaDispatcher INSTANCE = new NaiveReplicaDispatcher();

    private NaiveReplicaDispatcher() {
    }

    @Override
    public String chooseReplica(StoragePhysicalTask task) {
        if (task == null) {
            return null;
        }
        String masterStorageUnitId = task.getTargetFragment().getMasterStorageUnitId();
        StorageUnitMeta masterStorageUnit = DefaultMetaManager.getInstance().getStorageUnit(masterStorageUnitId);
        if (masterStorageUnit.getState() == StorageUnitState.DISCARD) {
            return masterStorageUnit.getMigrationTo();
        }
        return masterStorageUnitId;
    }

    public static NaiveReplicaDispatcher getInstance() {
        return INSTANCE;
    }
}
