package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import lombok.Data;

@Data
public class Statistics {

  private long id;

  private long startTime;

  private long endTime;

  private RequestContext context;

  public Statistics(long id, long startTime, long endTime, RequestContext context) {
    this.id = id;
    this.startTime = startTime;
    this.endTime = endTime;
    this.context = context;
  }
}
