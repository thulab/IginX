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

import java.sql.Time;
import java.util.Objects;

public final class TimeSeriesInterval implements Comparable<TimeSeriesInterval> {

    private String startTimeSeries;

    private String endTimeSeries;

    public TimeSeriesInterval(String startTimeSeries, String endTimeSeries) {
        this.startTimeSeries = startTimeSeries;
        this.endTimeSeries = endTimeSeries;
    }

    public String getStartTimeSeries() {
        return startTimeSeries;
    }

    public String getEndTimeSeries() {
        return endTimeSeries;
    }

    public void setStartTimeSeries(String startTimeSeries) {
        this.startTimeSeries = startTimeSeries;
    }

    public void setEndTimeSeries(String endTimeSeries) {
        this.endTimeSeries = endTimeSeries;
    }

    @Override
    public String toString() {
        return "" + startTimeSeries + "-" + endTimeSeries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeriesInterval that = (TimeSeriesInterval) o;
        return Objects.equals(startTimeSeries, that.startTimeSeries) && Objects.equals(endTimeSeries, that.endTimeSeries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeSeries, endTimeSeries);
    }

    public boolean isContain(String tsName) {
        return (startTimeSeries == null || (tsName != null && startTimeSeries.compareTo(tsName) <= 0))
                && (endTimeSeries == null || (tsName != null && endTimeSeries.compareTo(tsName) > 0));
    }

    public boolean isBefore(String tsName) {
        return tsName == null ? (startTimeSeries != null) : (startTimeSeries != null && tsName.compareTo(startTimeSeries) < 0);
    }

    public boolean isIntersect(TimeSeriesInterval tsInterval) {
        return (tsInterval.startTimeSeries == null || endTimeSeries == null || tsInterval.startTimeSeries.compareTo(endTimeSeries) < 0)
                && (tsInterval.endTimeSeries == null || startTimeSeries == null || tsInterval.endTimeSeries.compareTo(startTimeSeries) > 0);
    }

    public boolean isContain(TimeSeriesInterval tsInterval) {
        return (startTimeSeries == null || (tsInterval.startTimeSeries != null && startTimeSeries.compareTo(tsInterval.startTimeSeries) <= 0))
                && (endTimeSeries == null || (tsInterval.endTimeSeries != null && endTimeSeries.compareTo(tsInterval.endTimeSeries) >= 0));
    }

    public boolean isContainedBy(TimeSeriesInterval tsInterval) {
        return (tsInterval.startTimeSeries == null || (startTimeSeries != null && tsInterval.startTimeSeries.compareTo(startTimeSeries) <= 0))
                && (tsInterval.endTimeSeries == null || (endTimeSeries != null && tsInterval.endTimeSeries.compareTo(endTimeSeries) >= 0));
    }

    public static TimeSeriesInterval fromString(String str) {
        String[] parts = str.split("-");
        assert parts.length == 2;
        return new TimeSeriesInterval(parts[0].equals("null") ? null : parts[0], parts[1].equals("null") ? null : parts[1]);
    }

    @Override
    public int compareTo(TimeSeriesInterval o) {
        int value = compareTo(startTimeSeries, o.startTimeSeries);
        if (value != 0)
            return value;
        return compareTo(endTimeSeries, o.endTimeSeries);
    }

    private static int compareTo(String s1, String s2) {
        if (s1 == null && s2 == null)
            return 0;
        if (s1 == null)
            return -1;
        if (s2 == null)
            return 1;
        return s1.compareTo(s2);
    }
}
