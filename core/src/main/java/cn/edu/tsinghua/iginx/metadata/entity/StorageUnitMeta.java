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
package cn.edu.tsinghua.iginx.metadata.entity;

import java.util.ArrayList;
import java.util.List;

public final class StorageUnitMeta {

    private final String id;

    private final long storageEngineId;

    private final String masterId;

    private final boolean isMaster;

    private final transient List<StorageUnitMeta> replicas;

    public StorageUnitMeta(String id, long storageEngineId, String masterId, boolean isMaster) {
        this.id = id;
        this.storageEngineId = storageEngineId;
        this.masterId = masterId;
        this.isMaster = isMaster;
        this.replicas = new ArrayList<>();
    }

    public void addReplica(StorageUnitMeta storageUnit) {
        replicas.add(storageUnit);
    }

    public String getId() {
        return id;
    }

    public long getStorageEngineId() {
        return storageEngineId;
    }

    public String getMasterId() {
        return masterId;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public List<StorageUnitMeta> getReplicas() {
        return replicas;
    }
}
