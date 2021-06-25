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

import cn.edu.tsinghua.iginx.core.db.StorageEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class StorageEngineMeta {

    /**
     * 数据库的 id
     */
    private long id;

    /**
     * 时序数据库所在的 ip
     */
    private String ip;

    /**
     * 时序数据库开放的端口
     */
    private int port;

    /**
     * 时序数据库需要的其他参数信息，例如用户名、密码等
     */
    private Map<String, String> extraParams;

    /**
     * 数据库类型
     */
    private StorageEngine storageEngine;

    /**
     * 实例上管理的存储单元列表
     */
    private transient List<StorageUnitMeta> storageUnitList = new ArrayList<>();

    private long createdBy;

    public StorageEngineMeta(long id, String ip, int port, Map<String, String> extraParams, StorageEngine storageEngine, long createdBy) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.extraParams = extraParams;
        this.storageEngine = storageEngine;
        this.createdBy = createdBy;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Map<String, String> getExtraParams() {
        return extraParams;
    }

    public void setExtraParams(Map<String, String> extraParams) {
        this.extraParams = extraParams;
    }

    public StorageEngine getDbType() {
        return storageEngine;
    }

    public void setDbType(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    public List<StorageUnitMeta> getStorageUnitList() {
        if (storageUnitList == null) {
            storageUnitList = new ArrayList<>();
        }
        return storageUnitList;
    }

    public void removeStorageUnit(String id) {
        if (storageUnitList == null) {
            storageUnitList = new ArrayList<>();
        }
        storageUnitList.removeIf(e -> e.getId().equals(id));
    }

    public void addStorageUnit(StorageUnitMeta storageUnit) {
        if (storageUnitList == null) {
            storageUnitList = new ArrayList<>();
        }
        storageUnitList.add(storageUnit);
    }

    public long getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(long createdBy) {
        this.createdBy = createdBy;
    }
}
