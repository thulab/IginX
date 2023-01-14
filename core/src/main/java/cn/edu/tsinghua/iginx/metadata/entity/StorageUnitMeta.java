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

import cn.edu.tsinghua.iginx.conf.Constants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class StorageUnitMeta {

    private String id;

    private long storageEngineId;

    private String masterId;

    private final boolean isMaster;

    private long createdBy;

    private boolean initialStorageUnit = true;

    private boolean dummy = false;

    private boolean ifValid = true;

    private transient List<StorageUnitMeta> replicas = new ArrayList<>();

    public StorageUnitMeta(String id, long storageEngineId, String masterId, boolean isMaster) {
        this.id = id;
        this.storageEngineId = storageEngineId;
        this.masterId = masterId;
        this.isMaster = isMaster;
    }

    public StorageUnitMeta(String id, long storageEngineId) {
        this.id = id;
        this.storageEngineId = storageEngineId;
        this.masterId = id;
        this.isMaster = true;
        this.dummy = true;
        this.replicas = Collections.emptyList();
    }

    public StorageUnitMeta(String id, long storageEngineId, String masterId, boolean isMaster, boolean initialStorageUnit) {
        this.id = id;
        this.storageEngineId = storageEngineId;
        this.masterId = masterId;
        this.isMaster = isMaster;
        this.initialStorageUnit = initialStorageUnit;
    }

    public StorageUnitMeta(String id, long storageEngineId, String masterId, boolean isMaster, long createdBy, boolean initialStorageUnit, boolean dummy, List<StorageUnitMeta> replicas) {
        this.id = id;
        this.storageEngineId = storageEngineId;
        this.masterId = masterId;
        this.isMaster = isMaster;
        this.createdBy = createdBy;
        this.initialStorageUnit = initialStorageUnit;
        this.dummy = dummy;
        this.replicas = replicas;
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

    public void setId(String id) {
        this.id = id;
    }

    public long getStorageEngineId() {
        return storageEngineId;
    }

    public String getMasterId() {
        return masterId;
    }

    public void setMasterId(String masterId) {
        this.masterId = masterId;
    }

    public boolean isMaster() {
        return isMaster;
    }

    public List<StorageUnitMeta> getReplicas() {
        if (replicas == null)
            replicas = new ArrayList<>();
        return replicas;
    }

    public void setReplicas(List<StorageUnitMeta> replicas) {
        this.replicas = replicas;
    }

    public StorageUnitMeta renameStorageUnitMeta(String id, String masterId) {
        StorageUnitMeta storageUnitMeta = new StorageUnitMeta(id, storageEngineId, masterId, isMaster);
        storageUnitMeta.setCreatedBy(createdBy);
        storageUnitMeta.setInitialStorageUnit(initialStorageUnit);
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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("StorageUnitMeta: { id = ");
        builder.append(id);
        builder.append(", storageEngineId = ");
        builder.append(storageEngineId);
        builder.append(", masterId = ");
        builder.append(masterId);
        builder.append(", isMaster = ");
        builder.append(isMaster);
        builder.append(", createdBy = ");
        builder.append(createdBy);
        if (replicas != null) {
            builder.append(", replica id list = ");
            for (StorageUnitMeta storageUnit : replicas) {
                builder.append(" ");
                builder.append(storageUnit.getId());
            }
        }
        builder.append("}");
        return builder.toString();
    }

    public long getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(long createdBy) {
        this.createdBy = createdBy;
    }

    public boolean isInitialStorageUnit() {
        return initialStorageUnit;
    }

    public void setInitialStorageUnit(boolean initialStorageUnit) {
        this.initialStorageUnit = initialStorageUnit;
    }

    public void setStorageEngineId(long storageEngineId) {
        this.storageEngineId = storageEngineId;
    }

    public boolean isDummy() {
        return dummy;
    }

    public void setDummy(boolean dummy) {
        this.dummy = dummy;
    }

    public boolean isIfValid() {
        return ifValid;
    }

    public void setIfValid(boolean ifValid) {
        this.ifValid = ifValid;
    }

    public static String generateDummyStorageUnitID(long id) {
        return String.format(Constants.DUMMY + "%010d", (int)id);
    }
}
