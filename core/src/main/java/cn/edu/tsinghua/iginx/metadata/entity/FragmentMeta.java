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

    // 理论边界 左闭右开
    private TimeInterval idealTimeInterval;

    private TimeSeriesInterval idealTsInterval;

    // 实际边界 左闭右闭
    private TimeInterval actualTimeInterval;

    private TimeSeriesInterval actualTsInterval;

    private long createdBy;

    private long updatedBy;

    private String masterStorageUnitId;

    private transient StorageUnitMeta masterStorageUnit;

    private transient String fakeStorageUnitId;

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime) {
        this.idealTimeInterval = new TimeInterval(startTime, endTime);
        this.idealTsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
    }

    public FragmentMeta(String idealStartPrefix, String idealEndPrefix, long idealStartTime, long idealEndTime,
                        String actualStartPrefix, String actualEndPrefix, long actualStartTime, long actualEndTime,
                        String fakeStorageUnitId) {
        this.idealTsInterval = new TimeSeriesInterval(idealStartPrefix, idealEndPrefix);
        this.idealTimeInterval = new TimeInterval(idealStartTime, idealEndTime);
        this.actualTsInterval = new TimeSeriesInterval(actualStartPrefix, actualEndPrefix);
        this.actualTimeInterval = new TimeInterval(actualStartTime, actualEndTime);
        this.fakeStorageUnitId = fakeStorageUnitId;
    }

    public FragmentMeta(TimeSeriesInterval idealTsInterval, TimeInterval idealTimeInterval, TimeSeriesInterval actualTsInterval, TimeInterval actualTimeInterval, String fakeStorageUnitId) {
        this.idealTsInterval = idealTsInterval;
        this.idealTimeInterval = idealTimeInterval;
        this.actualTsInterval = actualTsInterval;
        this.actualTimeInterval = actualTimeInterval;
        this.fakeStorageUnitId = fakeStorageUnitId;
    }

    public FragmentMeta(String startPrefix, String endPrefix, long startTime, long endTime, StorageUnitMeta masterStorageUnit) {
        this.idealTimeInterval = new TimeInterval(startTime, endTime);
        this.idealTsInterval = new TimeSeriesInterval(startPrefix, endPrefix);
        this.masterStorageUnit = masterStorageUnit;
        this.masterStorageUnitId = masterStorageUnit.getMasterId();
    }

    public FragmentMeta(TimeSeriesInterval idealTsInterval, TimeInterval idealTimeInterval, TimeSeriesInterval actualTsInterval, TimeInterval actualTimeInterval, StorageUnitMeta masterStorageUnit) {
        this.idealTsInterval = idealTsInterval;
        this.idealTimeInterval = idealTimeInterval;
        this.actualTsInterval = actualTsInterval;
        this.actualTimeInterval = actualTimeInterval;
        this.masterStorageUnit = masterStorageUnit;
        this.masterStorageUnitId = masterStorageUnit.getMasterId();
    }

    public TimeInterval getIdealTimeInterval() {
        return idealTimeInterval;
    }

    public void setIdealTimeInterval(TimeInterval idealTimeInterval) {
        this.idealTimeInterval = idealTimeInterval;
    }

    public TimeSeriesInterval getIdealTsInterval() {
        return idealTsInterval;
    }

    public void setIdealTsInterval(TimeSeriesInterval idealTsInterval) {
        this.idealTsInterval = idealTsInterval;
    }

    public TimeInterval getActualTimeInterval() {
        return actualTimeInterval;
    }

    public void setActualTimeInterval(TimeInterval actualTimeInterval) {
        this.actualTimeInterval = actualTimeInterval;
    }

    public TimeSeriesInterval getActualTsInterval() {
        return actualTsInterval;
    }

    public void setActualTsInterval(TimeSeriesInterval actualTsInterval) {
        this.actualTsInterval = actualTsInterval;
    }

    public FragmentMeta endFragmentMeta(long endTime) {
        return new FragmentMeta(idealTsInterval.getStartTimeSeries(), idealTsInterval.getEndTimeSeries(), idealTimeInterval.getStartTime(), endTime);
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

    @Override
    public String toString() {
        return "FragmentMeta{"
                + "idealTimeInterval=" + idealTimeInterval
                + ", idealTimeSeriesInterval=" + idealTsInterval
                + ", actualTimeInterval=" + actualTimeInterval
                + ", actualTimeSeriesInterval=" + actualTsInterval
                + "}";
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
}
