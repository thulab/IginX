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
package cn.edu.tsinghua.iginx.split;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import lombok.Data;

@Data
public class SplitInfo {

    private TimeInterval timeInterval;

    private TimeSeriesInterval timeSeriesInterval;

    private StorageUnitMeta storageUnit;

    private FragmentMeta fragment;

    private IginxPlan.IginxPlanType type;

    private int combineGroup;

    public SplitInfo(TimeInterval timeInterval, TimeSeriesInterval timeSeriesInterval, StorageUnitMeta storageUnit) {
        this.timeInterval = timeInterval;
        this.timeSeriesInterval = timeSeriesInterval;
        this.storageUnit = storageUnit;
        this.type = IginxPlan.IginxPlanType.UNKNOWN;
    }

    public SplitInfo(TimeInterval timeInterval, TimeSeriesInterval timeSeriesInterval, StorageUnitMeta storageUnit,
                     FragmentMeta fragment) {
        this.timeInterval = timeInterval;
        this.timeSeriesInterval = timeSeriesInterval;
        this.storageUnit = storageUnit;
        this.fragment = fragment;
        this.type = IginxPlan.IginxPlanType.UNKNOWN;
    }

    public SplitInfo(TimeInterval timeInterval, TimeSeriesInterval timeSeriesInterval, StorageUnitMeta storageUnit,
                     IginxPlan.IginxPlanType type, int combineGroup) {
        this.timeInterval = timeInterval;
        this.timeSeriesInterval = timeSeriesInterval;
        this.storageUnit = storageUnit;
        this.type = type;
        this.combineGroup = combineGroup;
    }

    public TimeInterval getTimeInterval() {
        return timeInterval;
    }

    public void setTimeInterval(TimeInterval timeInterval) {
        this.timeInterval = timeInterval;
    }

    public TimeSeriesInterval getTimeSeriesInterval() {
        return timeSeriesInterval;
    }

    public void setTimeSeriesInterval(TimeSeriesInterval timeSeriesInterval) {
        this.timeSeriesInterval = timeSeriesInterval;
    }

    public StorageUnitMeta getStorageUnit() {
        return storageUnit;
    }

    public void setStorageUnit(StorageUnitMeta storageUnit) {
        this.storageUnit = storageUnit;
    }

    public IginxPlan.IginxPlanType getType() {
        return type;
    }

    public void setType(IginxPlan.IginxPlanType type) {
        this.type = type;
    }

    public int getCombineGroup() {
        return combineGroup;
    }

    public void setCombineGroup(int combineGroup) {
        this.combineGroup = combineGroup;
    }

    public FragmentMeta getFragment() {
        return fragment;
    }

    public void setFragment(FragmentMeta fragment) {
        this.fragment = fragment;
    }
}
