package cn.edu.tsinghua.iginx.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

@Slf4j
public class IginXDriver implements Driver {

    private static final boolean IGINX_JDBC_COMPLIANT = false;

    static {
        try {
            DriverManager.registerDriver(new IginXDriver());
        } catch (SQLException e) {
            log.error("Error occurs when registering IginX driver", e);
        }
    }

    private final String IGINX_URL_PREFIX = Config.IGINX_URL_PREFIX + ".*";

    public IginXDriver() {
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            return acceptsURL(url) ? new IginXConnection(url, info) : null;
        } catch (Exception e) {
            throw new SQLException("Connection Error, please check whether the network is available or the server has started.");
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return Pattern.matches(IGINX_URL_PREFIX, url);
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return IGINX_JDBC_COMPLIANT;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        throw new SQLFeatureNotSupportedException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(Constant.METHOD_NOT_SUPPORTED);
    }
}
