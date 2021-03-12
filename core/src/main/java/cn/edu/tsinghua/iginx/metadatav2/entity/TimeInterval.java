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
package cn.edu.tsinghua.iginx.metadatav2.entity;

import java.util.Objects;

public final class TimeInterval {

    private long beginTime;

    private long endTime;

    public TimeInterval(long beginTime, long endTime) {
        this.beginTime = beginTime;
        this.endTime = endTime;
    }

    public long getBeginTime() {
        return beginTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    @Override
    public String toString() {
        return beginTime + "-" + endTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeInterval that = (TimeInterval) o;
        return beginTime == that.beginTime && endTime == that.endTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(beginTime, endTime);
    }

    public boolean isIntersect(TimeInterval timeInterval) {
        return (timeInterval.beginTime < endTime) && (timeInterval.endTime > beginTime);
    }

    public boolean isBefore(TimeInterval timeInterval) {
        return endTime <= timeInterval.beginTime;
    }

    public boolean isAfter(TimeInterval timeInterval) {
        return beginTime >= timeInterval.endTime;
    }

    public boolean isContain(TimeInterval timeInterval) {
        return (beginTime <= timeInterval.beginTime) && (endTime >= timeInterval.endTime);
    }

    public boolean isContainedBy(TimeInterval timeInterval) {
        return (timeInterval.beginTime <= beginTime) && (timeInterval.endTime >= endTime);
    }

    public static TimeInterval fromString(String str) {
        String[] parts = str.split("-");
        assert parts.length == 2;
        return new TimeInterval(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
    }

}
