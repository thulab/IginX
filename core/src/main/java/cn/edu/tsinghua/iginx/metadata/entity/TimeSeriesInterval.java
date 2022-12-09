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

import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.Objects;

public final class TimeSeriesInterval implements TimeSeriesRange, Comparable<TimeSeriesRange> {

    private final TimeSeriesRange.TYPE type = TimeSeriesRange.TYPE.NORMAL;

    private String startTimeSeries;

    private String endTimeSeries;

    private String schemaPrefix = null;

    // 右边界是否为闭
    private boolean isClosed;

    public TimeSeriesInterval(String startTimeSeries, String endTimeSeries, boolean isClosed) {
        this.startTimeSeries = startTimeSeries;
        this.endTimeSeries = endTimeSeries;
        this.isClosed = isClosed;
    }

    public TimeSeriesInterval(String startTimeSeries, String endTimeSeries) {
        this(startTimeSeries, endTimeSeries, false);
    }

    public static TimeSeriesRange fromString(String str) {
        String[] parts = str.split("-");
        assert parts.length == 2;
        return new TimeSeriesInterval(parts[0].equals("null") ? null : parts[0], parts[1].equals("null") ? null : parts[1]);
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

    private String realTimeSeries(String timeSeries) {
        if (timeSeries != null && schemaPrefix != null) return schemaPrefix + "." + timeSeries;
        return timeSeries;
    }

    @Override
    public TYPE getType() {
        return type;
    }

    @Override
    public String getStartTimeSeries() {
        return startTimeSeries;
    }

    @Override
    public void setStartTimeSeries(String startTimeSeries) {
        this.startTimeSeries = startTimeSeries;
    }

    @Override
    public String getEndTimeSeries() {
        return endTimeSeries;
    }

    @Override
    public void setEndTimeSeries(String endTimeSeries) {
        this.endTimeSeries = endTimeSeries;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void setClosed(boolean closed) {
        isClosed = closed;
    }

    @Override
    public String toString() {
        return "" + startTimeSeries + "-" + endTimeSeries;
    }

    @Override
    public String getSchemaPrefix() {
        return schemaPrefix;
    }

    @Override
    public void setSchemaPrefix(String schemaPrefix) {
        this.schemaPrefix = schemaPrefix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeriesRange that = (TimeSeriesRange) o;
        return Objects.equals(startTimeSeries, that.getStartTimeSeries()) && Objects.equals(endTimeSeries, that.getEndTimeSeries());
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeSeries, endTimeSeries);
    }

    @Override
    public boolean isContain(String tsName) {
        //judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realTimeSeries(this.startTimeSeries);
        String endTimeSeries = realTimeSeries(this.endTimeSeries);

        return (startTimeSeries == null || (tsName != null && StringUtils.compare(tsName, startTimeSeries, true) >= 0))
                && (endTimeSeries == null || (tsName != null && StringUtils.compare(tsName, endTimeSeries, false) < 0));
    }

    public boolean isCompletelyBefore(String tsName) {
        //judge if is the dummy node && it will have specific prefix
        String endTimeSeries = realTimeSeries(this.endTimeSeries);

        return endTimeSeries != null && tsName != null && endTimeSeries.compareTo(tsName) <= 0;
    }

    @Override
    public boolean isIntersect(TimeSeriesRange tsInterval) {
        //judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realTimeSeries(this.startTimeSeries);
        String endTimeSeries = realTimeSeries(this.endTimeSeries);

        return (tsInterval.getStartTimeSeries() == null || endTimeSeries == null || StringUtils.compare(tsInterval.getStartTimeSeries(), endTimeSeries, false) < 0)
                && (tsInterval.getEndTimeSeries() == null || startTimeSeries == null || StringUtils.compare(tsInterval.getEndTimeSeries(), startTimeSeries, true) >= 0);
    }

    public TimeSeriesRange getIntersect(TimeSeriesRange tsInterval) {
        if (!isIntersect(tsInterval)) {
            return null;
        }
        //judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realTimeSeries(this.startTimeSeries);
        String endTimeSeries = realTimeSeries(this.endTimeSeries);

        String start = startTimeSeries == null ? tsInterval.getStartTimeSeries() :
                tsInterval.getStartTimeSeries() == null ? startTimeSeries :
                        StringUtils.compare(tsInterval.getStartTimeSeries(), startTimeSeries, true) < 0 ? startTimeSeries :
                                tsInterval.getStartTimeSeries();
        String end = endTimeSeries == null ? tsInterval.getEndTimeSeries() :
                tsInterval.getEndTimeSeries() == null ? endTimeSeries :
                        StringUtils.compare(tsInterval.getEndTimeSeries(), endTimeSeries, false) < 0 ? tsInterval.getEndTimeSeries() :
                                endTimeSeries;
        return new TimeSeriesInterval(start, end);
    }

    @Override
    public boolean isCompletelyAfter(TimeSeriesRange tsInterval) {
        //judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realTimeSeries(this.startTimeSeries);

        return tsInterval.getEndTimeSeries() != null && startTimeSeries != null && StringUtils.compare(tsInterval.getEndTimeSeries(), startTimeSeries, true) < 0;
    }

    @Override
    public boolean isAfter(String tsName) {
        //judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realTimeSeries(this.startTimeSeries);

        return startTimeSeries != null && StringUtils.compare(tsName, startTimeSeries, true) < 0;
    }

    @Override
    public int compareTo(TimeSeriesRange o) {
        //judge if is the dummy node && it will have specific prefix
        String startTimeSeries = realTimeSeries(this.startTimeSeries);
        String endTimeSeries = realTimeSeries(this.endTimeSeries);

        int value = compareTo(startTimeSeries, o.getStartTimeSeries());
        if (value != 0)
            return value;
        return compareTo(endTimeSeries, o.getEndTimeSeries());
    }
}
