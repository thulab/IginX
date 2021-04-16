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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FragmentMeta {

    private final TimeInterval timeInterval;

    private final TimeSeriesInterval tsInterval;

    /**
     * 所有的分片的信息
     */
    @Deprecated
    private Map<Integer, FragmentReplicaMeta> replicaMetas;

    private long createdBy;

    private long updatedBy;

    private String masterStorageUnitId;

    @Deprecated
    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime, Map<Integer, FragmentReplicaMeta> replicaMetas) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
        this.replicaMetas = replicaMetas;
    }

    @Deprecated
    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime, List<Long> storageEngineIdList) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
        Map<Integer, FragmentReplicaMeta> replicaMetas = new HashMap<>();
        for (int i = 0; i < storageEngineIdList.size(); i++) {
            replicaMetas.put(i, new FragmentReplicaMeta(this.timeInterval, this.tsInterval, i, storageEngineIdList.get(i)));
        }
        this.replicaMetas = Collections.unmodifiableMap(replicaMetas);
    }

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime, String masterStorageUnitId) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
        this.masterStorageUnitId = masterStorageUnitId;
    }

    public TimeInterval getTimeInterval() {
        return timeInterval;
    }

    public TimeSeriesInterval getTsInterval() {
        return tsInterval;
    }

    @Deprecated
    public Map<Integer, FragmentReplicaMeta> getReplicaMetas() {
        return new HashMap<>(replicaMetas);
    }

    @Deprecated
    public int getReplicaMetasNum() {
        return replicaMetas.size();
    }

    public FragmentMeta endFragmentMeta(long endTime) {
        return new FragmentMeta(tsInterval.getStartTimeSeries(), tsInterval.getEndTimeSeries(), timeInterval.getStartTime(), endTime, replicaMetas);
    }

    public long getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(long createdBy) {
        this.createdBy = createdBy;
    }

    public long getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(long updatedBy) {
        this.updatedBy = updatedBy;
    }

    @Override
    public String toString() {
        return "FragmentMeta{" +
                "timeInterval=" + timeInterval +
                ", tsInterval=" + tsInterval +
                '}';
    }

    public String getMasterStorageUnitId() {
        return masterStorageUnitId;
    }
}
