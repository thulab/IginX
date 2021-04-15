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

public final class FragmentReplicaMeta {

    private final TimeInterval timeInterval;

    private final TimeSeriesInterval tsInterval;

    /**
     * 分片副本的序号，如果该值为 0，则意味其为主分片
     */
    private final int replicaIndex;

    /**
     * 当前分片副本所在的数据库
     */
    private final long storageEngineId;

    private final String storageUnitId;

    public FragmentReplicaMeta(TimeInterval timeInterval, TimeSeriesInterval tsInterval, int replicaIndex, long storageEngineId) {
        this.timeInterval = timeInterval;
        this.tsInterval = tsInterval;
        this.replicaIndex = replicaIndex;
        this.storageEngineId = storageEngineId;
        this.storageUnitId = "";
    }

    public FragmentReplicaMeta(TimeInterval timeInterval, TimeSeriesInterval tsInterval, int replicaIndex, String storageUnitId) {
        this.timeInterval = timeInterval;
        this.tsInterval = tsInterval;
        this.replicaIndex = replicaIndex;
        this.storageEngineId = 0L;
        this.storageUnitId = storageUnitId;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public long getStorageEngineId() {
        return storageEngineId;
    }

    public TimeInterval getTimeInterval() {
        return timeInterval;
    }

    public TimeSeriesInterval getTsInterval() {
        return tsInterval;
    }

    public long getStartTime() {
        return timeInterval.getStartTime();
    }

    public long getEndTime() {
        return timeInterval.getEndTime();
    }

    public String getStorageUnitId() {
        return storageUnitId;
    }
}
