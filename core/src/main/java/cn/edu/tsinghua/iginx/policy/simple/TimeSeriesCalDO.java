package cn.edu.tsinghua.iginx.policy.simple;

import lombok.Data;

@Data
public class TimeSeriesCalDO implements Comparable<TimeSeriesCalDO> {

  private String timeSeries;

  private Long recentTimeStamp = 0L;

  private Long firstTimestamp = Long.MAX_VALUE;

  private Long lastTimestamp = Long.MIN_VALUE;

  private Integer count = 0;

  private Long totalByte = 0L;

  public Double getValue() {
    double ret = 0.0;
    if (count > 1 && lastTimestamp > firstTimestamp) {
      ret = 1.0 * totalByte / count * (count - 1) / (lastTimestamp - firstTimestamp);
    }
    return ret;
  }

  @Override
  public int compareTo(TimeSeriesCalDO timeSeriesCalDO) {
    if (getValue() < timeSeriesCalDO.getValue()) {
      return -1;
    } else if (getValue() > timeSeriesCalDO.getValue()) {
      return 1;
    }
    return 0;
  }

  public void merge(Long recentTimeStamp, Long firstTimestamp, Long lastTimestamp, Integer count,
      Long totalByte) {
    this.recentTimeStamp = recentTimeStamp;
    this.firstTimestamp = Math.min(firstTimestamp, this.firstTimestamp);
    this.lastTimestamp = Math.max(lastTimestamp, this.lastTimestamp);
    this.count += count;
    this.totalByte += totalByte;
  }
}
