package cn.edu.tsinghua.iginx.jdbc;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class IginXDataSource implements DataSource {

  private String url;
  private String user;
  private String password;
  private Properties properties;
  private Integer port = 6888;

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
    properties.setProperty(Config.USER, user);
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
    properties.setProperty(Config.PASSWORD, user);
  }

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  @Override
  public Connection getConnection() throws SQLException {
    try {
      return new IginXConnection(url, properties);
    } catch (SessionException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    try {
      Properties newProp = new Properties();
      newProp.setProperty(Config.USER, username);
      newProp.setProperty(Config.PASSWORD, password);
      return new IginXConnection(url, newProp);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Logger getParentLogger() {
    return null;
  }
}
