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

public final class StorageEngineStatistics {

    private double density;

    private double capacity;

    private double saturation;

    private long maxEndTime;

    public StorageEngineStatistics() {

    }

    public StorageEngineStatistics(double density, double capacity, double saturation, long maxEndTime) {
        this.density = density;
        this.capacity = capacity;
        this.saturation = saturation;
        this.maxEndTime = maxEndTime;
    }

    public void updateByTimeSeriesStatistics(TimeSeriesStatistics timeSeriesStatistics) {
        density += timeSeriesStatistics.getWriteBytes();
        saturation += timeSeriesStatistics.getWriteBytes();
    }

    public void updateByStorageEngineStatistics(StorageEngineStatistics storageEngineStatistics) {
        density += storageEngineStatistics.getDensity();
        saturation += storageEngineStatistics.getSaturation();
        maxEndTime = Math.max(maxEndTime, storageEngineStatistics.getMaxEndTime());
    }

    public double getDensity() {
        return density;
    }

    public void setDensity(double density) {
        this.density = density;
    }

    public double getCapacity() {
        return capacity;
    }

    public void setCapacity(double capacity) {
        this.capacity = capacity;
    }

    public double getSaturation() {
        return saturation;
    }

    public void setSaturation(double saturation) {
        this.saturation = saturation;
    }

    public long getMaxEndTime() {
        return maxEndTime;
    }

    public void setMaxEndTime(long maxEndTime) {
        this.maxEndTime = maxEndTime;
    }

    @Override
    public String toString() {
        return "StorageEngineStatistics{" +
                "density=" + density +
                ", capacity=" + capacity +
                ", saturation=" + saturation +
                ", maxEndTime=" + maxEndTime +
                '}';
    }
}
