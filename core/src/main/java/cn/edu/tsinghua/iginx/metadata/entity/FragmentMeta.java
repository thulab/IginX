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

import java.util.Objects;

public final class FragmentMeta {

    private final TimeInterval timeInterval;

    private final TimeSeriesRange tsInterval;

    private long createdBy;

    private long updatedBy;

    private String masterStorageUnitId;

    private transient StorageUnitMeta masterStorageUnit;

    private transient String fakeStorageUnitId;

    private boolean initialFragment = true;

    private boolean dummyFragment = false;

    private boolean valid = true;

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
    }

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime, String fakeStorageUnitId) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
        this.fakeStorageUnitId = fakeStorageUnitId;
    }

    public FragmentMeta(TimeSeriesRange tsInterval, TimeInterval timeInterval, String fakeStorageUnitId) {
        this.timeInterval = timeInterval;
        this.tsInterval = tsInterval;
        this.fakeStorageUnitId = fakeStorageUnitId;
    }

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime, StorageUnitMeta masterStorageUnit) {
        this.timeInterval = new TimeInterval(startTime, endTime);
        this.tsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
        this.masterStorageUnit = masterStorageUnit;
        this.masterStorageUnitId = masterStorageUnit.getMasterId();
    }

    public FragmentMeta(TimeSeriesRange tsInterval, TimeInterval timeInterval, StorageUnitMeta masterStorageUnit) {
        this.timeInterval = timeInterval;
        this.tsInterval = tsInterval;
        this.masterStorageUnit = masterStorageUnit;
        this.masterStorageUnitId = masterStorageUnit.getMasterId();
    }

    public FragmentMeta(TimeInterval timeInterval, TimeSeriesRange tsInterval, long createdBy, long updatedBy, String masterStorageUnitId, StorageUnitMeta masterStorageUnit, String fakeStorageUnitId, boolean initialFragment, boolean dummyFragment) {
        this.timeInterval = timeInterval;
        this.tsInterval = tsInterval;
        this.createdBy = createdBy;
        this.updatedBy = updatedBy;
        this.masterStorageUnitId = masterStorageUnitId;
        this.masterStorageUnit = masterStorageUnit;
        this.fakeStorageUnitId = fakeStorageUnitId;
        this.initialFragment = initialFragment;
        this.dummyFragment = dummyFragment;
    }

    public TimeInterval getTimeInterval() {
        return timeInterval;
    }

    public TimeSeriesRange getTsInterval() {
        return tsInterval;
    }

    public FragmentMeta endFragmentMeta(long endTime) {
        FragmentMeta fragment = new FragmentMeta(tsInterval.getStartTimeSeries(), tsInterval.getEndTimeSeries(), timeInterval.getStartTime(), endTime);
        fragment.setMasterStorageUnit(masterStorageUnit);
        fragment.setMasterStorageUnitId(masterStorageUnitId);
        fragment.setCreatedBy(createdBy);
        fragment.setInitialFragment(initialFragment);
        return fragment;
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
        this.masterStorageUnitId = masterStorageUnit.getMasterId();
    }

    public String getFakeStorageUnitId() {
        return fakeStorageUnitId;
    }

    public void setFakeStorageUnitId(String fakeStorageUnitId) {
        this.fakeStorageUnitId = fakeStorageUnitId;
    }

    public String getMasterStorageUnitId() {
        return masterStorageUnitId;
    }

    public void setMasterStorageUnitId(String masterStorageUnitId) {
        this.masterStorageUnitId = masterStorageUnitId;
    }

    public boolean isValid() {
        return valid;
    }

    public void setIfValid(boolean ifValid) {
        this.valid = ifValid;
    }

    @Override
    public String toString() {
        return "FragmentMeta{" +
            "timeInterval=" + timeInterval +
            ", tsInterval=" + tsInterval +
            ", masterStorageUnitId=" + masterStorageUnitId +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FragmentMeta that = (FragmentMeta) o;
        return Objects.equals(timeInterval, that.timeInterval) && Objects.equals(tsInterval, that.tsInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeInterval, tsInterval);
    }

    public boolean isInitialFragment() {
        return initialFragment;
    }

    public void setInitialFragment(boolean initialFragment) {
        this.initialFragment = initialFragment;
    }

    public boolean isDummyFragment() {
        return dummyFragment;
    }

    public void setDummyFragment(boolean dummyFragment) {
        this.dummyFragment = dummyFragment;
    }

    public static long sizeOf() {
        return 62L;
    }
}
