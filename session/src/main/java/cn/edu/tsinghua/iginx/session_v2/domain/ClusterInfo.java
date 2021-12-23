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
package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.thrift.IginxInfo;
import cn.edu.tsinghua.iginx.thrift.LocalMetaStorageInfo;
import cn.edu.tsinghua.iginx.thrift.MetaStorageInfo;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;

import java.util.List;

public final class ClusterInfo {

    private final List<IginxInfo> iginxInfos;

    private final List<StorageEngineInfo> storageEngineInfos;

    private final LocalMetaStorageInfo localMetaStorageInfo;

    private final List<MetaStorageInfo> metaStorageInfos;

    public ClusterInfo(List<IginxInfo> iginxInfos, List<StorageEngineInfo> storageEngineInfos, LocalMetaStorageInfo localMetaStorageInfo, List<MetaStorageInfo> metaStorageInfos) {
        this.iginxInfos = iginxInfos;
        this.storageEngineInfos = storageEngineInfos;
        this.localMetaStorageInfo = localMetaStorageInfo;
        this.metaStorageInfos = metaStorageInfos;
    }

    public ClusterInfo(List<IginxInfo> iginxInfos, List<StorageEngineInfo> storageEngineInfos, LocalMetaStorageInfo localMetaStorageInfo) {
        this(iginxInfos, storageEngineInfos, localMetaStorageInfo, null);
    }

    public ClusterInfo(List<IginxInfo> iginxInfos, List<StorageEngineInfo> storageEngineInfos, List<MetaStorageInfo> metaStorageInfo) {
        this(iginxInfos, storageEngineInfos, null, metaStorageInfo);
    }

    public List<IginxInfo> getIginxInfos() {
        return iginxInfos;
    }

    public List<StorageEngineInfo> getStorageEngineInfos() {
        return storageEngineInfos;
    }

    public LocalMetaStorageInfo getLocalMetaStorageInfo() {
        return localMetaStorageInfo;
    }

    public List<MetaStorageInfo> getMetaStorageInfos() {
        return metaStorageInfos;
    }

    public boolean isUseLocalMetaStorage() {
        return localMetaStorageInfo != null;
    }

}
