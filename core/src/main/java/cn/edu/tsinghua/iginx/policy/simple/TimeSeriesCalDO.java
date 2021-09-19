package cn.edu.tsinghua.iginx.policy.simple;

import lombok.Data;

@Data
public class TimeSeriesCalDO implements Comparable<TimeSeriesCalDO>
{
    private Long recentTimeStamp;

    private String timeSeries;

    private Long firstTimestamp;

    private Long lastTimestamp;

    private Long totalByte;

    private Integer count;

    public Double getValue() {
        double ret = 0.0;
        if (count > 1 && lastTimestamp > firstTimestamp) {
            ret = 1.0 * totalByte / count * (count - 1) / (lastTimestamp  - firstTimestamp);
        }
        return ret;
    }

    @Override
    public int compareTo(TimeSeriesCalDO timeSeriesCalDO)
    {
        if (getValue() < timeSeriesCalDO.getValue())
            return -1;
        else if (getValue() > timeSeriesCalDO.getValue())
            return 1;
        return 0;
    }
}
