package cn.edu.tsinghua.iginx.jdbc;

import java.sql.SQLException;

public class IginxUrlException extends SQLException {

  private static final long serialVersionUID = 4869157182500152406L;

  public IginxUrlException(String reason) {
    super(reason);
  }
}
