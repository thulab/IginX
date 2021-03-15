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

public final class TimeSeriesInterval {

    private String beginTimeSeries;

    private String endTimeSeries;

    public TimeSeriesInterval(String beginTimeSeries, String endTimeSeries) {
        this.beginTimeSeries = beginTimeSeries;
        this.endTimeSeries = endTimeSeries;
    }

    public String getBeginTimeSeries() {
        return beginTimeSeries;
    }

    public String getEndTimeSeries() {
        return endTimeSeries;
    }

    public void setBeginTimeSeries(String beginTimeSeries) {
        this.beginTimeSeries = beginTimeSeries;
    }

    public void setEndTimeSeries(String endTimeSeries) {
        this.endTimeSeries = endTimeSeries;
    }

    @Override
    public String toString() {
        return "" + beginTimeSeries + "-" + endTimeSeries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeriesInterval that = (TimeSeriesInterval) o;
        return Objects.equals(beginTimeSeries, that.beginTimeSeries) && Objects.equals(endTimeSeries, that.endTimeSeries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(beginTimeSeries, endTimeSeries);
    }

    public boolean isContain(String tsName) {
        return (beginTimeSeries == null || (tsName != null && beginTimeSeries.compareTo(tsName) <= 0))
                && (endTimeSeries == null || (tsName != null && endTimeSeries.compareTo(tsName) > 0));
    }

    public boolean isBefore(String tsName) {
        return tsName == null ? (beginTimeSeries != null) : (beginTimeSeries != null && tsName.compareTo(beginTimeSeries) < 0);
    }

    public boolean isIntersect(TimeSeriesInterval tsInterval) {
        return (tsInterval.beginTimeSeries == null || endTimeSeries == null || tsInterval.beginTimeSeries.compareTo(endTimeSeries) < 0)
                && (tsInterval.endTimeSeries == null || beginTimeSeries == null || tsInterval.endTimeSeries.compareTo(beginTimeSeries) > 0);
    }

    public boolean isContain(TimeSeriesInterval tsInterval) {
        return (beginTimeSeries == null || (tsInterval.beginTimeSeries != null && beginTimeSeries.compareTo(tsInterval.beginTimeSeries) <= 0))
                && (endTimeSeries == null || (tsInterval.endTimeSeries != null && endTimeSeries.compareTo(tsInterval.endTimeSeries) >= 0));
    }

    public boolean isContainedBy(TimeSeriesInterval tsInterval) {
        return (tsInterval.beginTimeSeries == null || (beginTimeSeries != null && tsInterval.beginTimeSeries.compareTo(beginTimeSeries) <= 0))
                && (tsInterval.endTimeSeries == null || (endTimeSeries != null && tsInterval.endTimeSeries.compareTo(endTimeSeries) >= 0));
    }

    public static TimeSeriesInterval fromString(String str) {
        String[] parts = str.split("-");
        assert parts.length == 2;
        return new TimeSeriesInterval(parts[0].equals("null") ? null : parts[0], parts[1].equals("null") ? null : parts[1]);
    }

}
