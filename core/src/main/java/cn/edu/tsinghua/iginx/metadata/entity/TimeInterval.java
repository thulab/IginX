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

public final class TimeInterval {

    private long startTime;

    private long endTime;

    public TimeInterval(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return "" + startTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeInterval that = (TimeInterval) o;
        return startTime == that.startTime && endTime == that.endTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTime, endTime);
    }

    public boolean isIntersect(TimeInterval timeInterval) {
        return (timeInterval.startTime < endTime) && (timeInterval.endTime > startTime);
    }

    public boolean isBefore(TimeInterval timeInterval) {
        return endTime <= timeInterval.startTime;
    }

    public boolean isAfter(TimeInterval timeInterval) {
        return startTime >= timeInterval.endTime;
    }

    public boolean isContain(TimeInterval timeInterval) {
        return (startTime <= timeInterval.startTime) && (endTime >= timeInterval.endTime);
    }

    public boolean isContainedBy(TimeInterval timeInterval) {
        return (timeInterval.startTime <= startTime) && (timeInterval.endTime >= endTime);
    }

}
