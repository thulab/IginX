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

public final class FragmentMeta {

    private final TimeInterval timeInterval;

    private final TimeSeriesInterval tsInterval;

    private long createdBy;

    private long updatedBy;

    private StorageUnitMeta masterStorageUnit;

    private String fakeStorageUnitId;

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
    }

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime, String fakeStorageUnitId) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
        this.fakeStorageUnitId = fakeStorageUnitId;
    }

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime, StorageUnitMeta masterStorageUnit) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
        this.masterStorageUnit = masterStorageUnit;
    }

    public TimeInterval getTimeInterval() {
        return timeInterval;
    }

    public TimeSeriesInterval getTsInterval() {
        return tsInterval;
    }

    public FragmentMeta endFragmentMeta(long endTime) {
        return new FragmentMeta(tsInterval.getStartTimeSeries(), tsInterval.getEndTimeSeries(), timeInterval.getStartTime(), endTime);
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

    public StorageUnitMeta getMasterStorageUnit() {
        return masterStorageUnit;
    }

    public void setMasterStorageUnit(StorageUnitMeta masterStorageUnit) {
        this.masterStorageUnit = masterStorageUnit;
    }

    public String getFakeStorageUnitId() {
        return fakeStorageUnitId;
    }

    public void setFakeStorageUnitId(String fakeStorageUnitId) {
        this.fakeStorageUnitId = fakeStorageUnitId;
    }

    @Override
    public String toString() {
        return "FragmentMeta{" +
                "timeInterval=" + timeInterval +
                ", tsInterval=" + tsInterval +
                '}';
    }
}
