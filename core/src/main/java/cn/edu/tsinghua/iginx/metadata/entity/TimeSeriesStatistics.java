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

public final class TimeSeriesStatistics {

    private long writeBytes;

    private long readBytes;

    private long totalBytes;

    private double density;

    private long storageEngineId;

    public TimeSeriesStatistics() {

    }

    public TimeSeriesStatistics(long writeBytes, long storageEngineId) {
        this.writeBytes = writeBytes;
        this.storageEngineId = storageEngineId;
    }

    public TimeSeriesStatistics(long writeBytes, long readBytes, long storageEngineId) {
        this.writeBytes = writeBytes;
        this.readBytes = readBytes;
        this.storageEngineId = storageEngineId;
    }

    public synchronized void update(TimeSeriesStatistics timeSeriesStatistics) {
        writeBytes += timeSeriesStatistics.getWriteBytes();
        readBytes += timeSeriesStatistics.getReadBytes();
        totalBytes += timeSeriesStatistics.getTotalBytes();
    }

    public long getWriteBytes() {
        return writeBytes;
    }

    public void setWriteBytes(long writeBytes) {
        this.writeBytes = writeBytes;
    }

    public long getReadBytes() {
        return readBytes;
    }

    public void setReadBytes(long readBytes) {
        this.readBytes = readBytes;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public long getStorageEngineId() {
        return storageEngineId;
    }

    public void setStorageEngineId(long storageEngineId) {
        this.storageEngineId = storageEngineId;
    }

    public double getDensity() {
        return density;
    }

    public void setDensity(double density) {
        this.density = density;
    }
}
