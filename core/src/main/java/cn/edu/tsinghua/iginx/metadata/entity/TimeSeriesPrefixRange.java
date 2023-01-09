package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.alibaba.fastjson2.annotation.JSONType;

@JSONType(typeName = "TimeSeriesPrefixRange")
public class TimeSeriesPrefixRange implements TimeSeriesRange {

    private String timeSeries;

    private final TYPE type = TYPE.PREFIX;

    private boolean isClosed;

    private String schemaPrefix = null;

    public TimeSeriesPrefixRange(String timeSeries) {
        this.timeSeries = timeSeries;
    }

    public TimeSeriesPrefixRange(String timeSeries, String schemaPrefix) {
        this.timeSeries = timeSeries;
        this.schemaPrefix = schemaPrefix;
    }

    public TimeSeriesPrefixRange(String timeSeries, boolean isClosed) {
        this.timeSeries = timeSeries;
        this.isClosed = isClosed;
    }

    private String realTimeSeries(String timeSeries) {
        if (timeSeries != null && schemaPrefix != null) return schemaPrefix + "." + timeSeries;
        return timeSeries;
    }

    @Override
    public boolean isContain(String tsName) {
        //judge if is the dummy node && it will have specific prefix
        String timeSeries = realTimeSeries(this.timeSeries);

        return (timeSeries == null || (tsName != null && StringUtils.compare(tsName, timeSeries) == 0));
    }

    @Override
    public boolean isIntersect(TimeSeriesRange tsInterval) {
        //judge if is the dummy node && it will have specific prefix
        String timeSeries = realTimeSeries(this.timeSeries);

        return (tsInterval.getStartTimeSeries() == null || timeSeries == null || StringUtils.compare(tsInterval.getStartTimeSeries(), timeSeries) <= 0)
                && (tsInterval.getEndTimeSeries() == null || timeSeries == null || StringUtils.compare(tsInterval.getEndTimeSeries(), timeSeries) >= 0);
    }

    @Override
    public String getTimeSeries() {
        return timeSeries;
    }

    @Override
    public void setTimeSeries(String timeSeries) {
        this.timeSeries = timeSeries;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void setClosed(boolean closed) {
        this.isClosed = closed;
    }

    @Override
    public TYPE getType() {
        return type;
    }

    @Override
    public int compareTo(TimeSeriesRange o) {
        return 0;
    }

    @Override
    public String getSchemaPrefix() {
        return schemaPrefix;
    }

    @Override
    public void setSchemaPrefix(String schemaPrefix) {
        this.schemaPrefix = schemaPrefix;
    }
}