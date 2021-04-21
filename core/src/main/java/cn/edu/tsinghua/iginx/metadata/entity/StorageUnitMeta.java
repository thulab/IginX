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
import java.util.Objects;

public final class StorageUnitMeta {

    private final String id;

    private final long storageEngineId;

    private final String masterId;

    private final boolean isMaster;

    private long createdBy;

    private transient List<StorageUnitMeta> replicas = new ArrayList<>();

    public StorageUnitMeta(String id, long storageEngineId, String masterId, boolean isMaster) {
        this.id = id;
        this.storageEngineId = storageEngineId;
        this.masterId = masterId;
        this.isMaster = isMaster;
    }

    public void addReplica(StorageUnitMeta storageUnit) {
        if (replicas == null)
            replicas = new ArrayList<>();
        replicas.add(storageUnit);
    }

    public void removeReplica(StorageUnitMeta storageUnit) {
        if (replicas == null)
            replicas = new ArrayList<>();
        replicas.remove(storageUnit);
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

    public void setReplicas(List<StorageUnitMeta> replicas) {
        this.replicas = replicas;
    }

    public List<StorageUnitMeta> getReplicas() {
        if (replicas == null)
            replicas = new ArrayList<>();
        return replicas;
    }

    public StorageUnitMeta migrateStorageUnitMeta(long targetStorageEngineId) {
        StorageUnitMeta storageUnitMeta = new StorageUnitMeta(id, targetStorageEngineId, masterId, isMaster);
        storageUnitMeta.setCreatedBy(createdBy);
        return storageUnitMeta;
    }

    public StorageUnitMeta renameStorageUnitMeta(String id, String masterId) {
        StorageUnitMeta storageUnitMeta = new StorageUnitMeta(id, storageEngineId, masterId, isMaster);
        storageUnitMeta.setCreatedBy(createdBy);
        return storageUnitMeta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StorageUnitMeta that = (StorageUnitMeta) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public long getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(long createdBy) {
        this.createdBy = createdBy;
    }
}
