package cn.edu.tsinghua.iginx.metadata.entity;

import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeSeriesIntervalInPrefix implements TimeSeriesInterval {

    private static final Logger logger = LoggerFactory.getLogger(TimeSeriesIntervalInPrefix.class);

    private String timeSeries;

    private final TYPE type = TYPE.PREFIX;

    private boolean isClosed;

    private String schemaPrefix = null;

    public TimeSeriesIntervalInPrefix(String timeSeries) {
        this.timeSeries = timeSeries;
    }

    public TimeSeriesIntervalInPrefix(String timeSeries, boolean isClosed) {
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
    public boolean isIntersect(TimeSeriesInterval tsInterval) {
        //judge if is the dummy node && it will have specific prefix
        String timeSeries = realTimeSeries(this.timeSeries);

        return (tsInterval.getStartTimeSeries() == null || timeSeries == null || StringUtils.compare(tsInterval.getStartTimeSeries(), timeSeries) <= 0)
                && (tsInterval.getEndTimeSeries() == null || timeSeries == null || StringUtils.compare(tsInterval.getEndTimeSeries(), timeSeries) >= 0);
    }

    @Override
    public boolean isCompletelyAfter(TimeSeriesInterval tsInterval) {
        return false;
    }

    @Override
    public boolean isAfter(String tsName) {
        return false;
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
    public int compareTo(TimeSeriesInterval o) {
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