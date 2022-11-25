package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class TimeSeriesIntervalNormal implements TimeSeriesInterval, Comparable<TimeSeriesInterval> {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesIntervalNormal.class);

    private final TimeSeriesInterval.TYPE type = TimeSeriesInterval.TYPE.NORMAL;

    private String startTimeSeries;

    private String endTimeSeries;

    // 右边界是否为闭
    private boolean isClosed;

    public TimeSeriesIntervalNormal(String startTimeSeries, String endTimeSeries, boolean isClosed) {
        this.startTimeSeries = startTimeSeries;
        this.endTimeSeries = endTimeSeries;
        this.isClosed = isClosed;
    }

    public TimeSeriesIntervalNormal(String startTimeSeries, String endTimeSeries) {
        this(startTimeSeries, endTimeSeries, false);
    }

    public static TimeSeriesInterval fromString(String str) {
        String[] parts = str.split("-");
        assert parts.length == 2;
        return new TimeSeriesIntervalNormal(parts[0].equals("null") ? null : parts[0], parts[1].equals("null") ? null : parts[1]);
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeriesInterval that = (TimeSeriesInterval) o;
        return Objects.equals(startTimeSeries, that.getStartTimeSeries()) && Objects.equals(endTimeSeries, that.getEndTimeSeries());
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeSeries, endTimeSeries);
    }

    @Override
    public boolean isContain(String tsName) {
        return (startTimeSeries == null || (tsName != null && StringUtils.compare(tsName, startTimeSeries, true) >= 0))
                && (endTimeSeries == null || (tsName != null && StringUtils.compare(tsName, endTimeSeries, false) < 0));
    }

    public boolean isCompletelyBefore(String tsName) {
        return endTimeSeries != null && tsName != null && endTimeSeries.compareTo(tsName) <= 0;
    }

    @Override
    public boolean isIntersect(TimeSeriesInterval tsInterval) {
        return (tsInterval.getStartTimeSeries() == null || endTimeSeries == null || StringUtils.compare(tsInterval.getStartTimeSeries(), endTimeSeries, false) < 0)
                && (tsInterval.getEndTimeSeries() == null || startTimeSeries == null || StringUtils.compare(tsInterval.getEndTimeSeries(), startTimeSeries, true) >= 0);
    }

    public TimeSeriesInterval getIntersect(TimeSeriesInterval tsInterval) {
        if (!isIntersect(tsInterval)) {
            return null;
        }
        String start = startTimeSeries == null ? tsInterval.getStartTimeSeries() :
                tsInterval.getStartTimeSeries() == null ? startTimeSeries :
                        StringUtils.compare(tsInterval.getStartTimeSeries(), startTimeSeries, true) < 0 ? startTimeSeries :
                                tsInterval.getStartTimeSeries();
        String end = endTimeSeries == null ? tsInterval.getEndTimeSeries() :
                tsInterval.getEndTimeSeries() == null ? endTimeSeries :
                        StringUtils.compare(tsInterval.getEndTimeSeries(), endTimeSeries, false) < 0 ? tsInterval.getEndTimeSeries() :
                                endTimeSeries;
        return new TimeSeriesIntervalNormal(start, end);
    }

    @Override
    public boolean isCompletelyAfter(TimeSeriesInterval tsInterval) {
        return tsInterval.getEndTimeSeries() != null && startTimeSeries != null && StringUtils.compare(tsInterval.getEndTimeSeries(), startTimeSeries, true) < 0;
    }

    @Override
    public boolean isAfter(String tsName) {
        return startTimeSeries != null && StringUtils.compare(tsName, startTimeSeries, true) < 0;
    }

    @Override
    public int compareTo(TimeSeriesInterval o) {
        int value = compareTo(startTimeSeries, o.getStartTimeSeries());
        if (value != 0)
            return value;
        return compareTo(endTimeSeries, o.getEndTimeSeries());
    }
}
