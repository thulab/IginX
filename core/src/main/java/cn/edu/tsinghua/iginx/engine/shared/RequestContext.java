package cn.edu.tsinghua.iginx.engine.shared;

import cn.edu.tsinghua.iginx.monitor.NodePerformanceMonitor;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class RequestContext {

  private long id;

  private long startTime;

  private long endTime;

  private long sessionId;

  private Map<String, Object> extraParams;

  private Status status;

  private String sql;

  private boolean fromSQL;

  private SqlType sqlType;

  private Statement statement;

  private Result result;

  private void init() {
    this.id = SnowFlakeUtils.getInstance().nextId();
    this.startTime = System.currentTimeMillis();
    this.extraParams = new HashMap<>();
  }

  public RequestContext(long sessionId) {
    init();
    this.sessionId = sessionId;
  }

  public RequestContext(long sessionId, Statement statement) {
    init();
    this.sessionId = sessionId;
    this.statement = statement;
    this.fromSQL = false;
  }

  public RequestContext(long sessionId, String sql) {
    init();
    this.sessionId = sessionId;
    this.sql = sql;
    this.fromSQL = true;
    this.sqlType = SqlType.Unknown;
  }

  public Object getExtraParam(String key) {
    return extraParams.getOrDefault(key, null);
  }

  public void setExtraParam(String key, Object value) {
    extraParams.put(key, value);
  }

  public Result getResult() {
    endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    if (sqlType == SqlType.Insert) {
      NodePerformanceMonitor.getInstance().recordWrite(duration);
    } else if (sqlType == SqlType.Query) {
      NodePerformanceMonitor.getInstance().recordRead(duration);
    }
    return result;
  }
}
