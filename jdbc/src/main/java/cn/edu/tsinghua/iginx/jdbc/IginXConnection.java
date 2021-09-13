package cn.edu.tsinghua.iginx.jdbc;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class IginXConnection implements Connection {

    private Session session;
    private IginXConnectionParams params;

    private boolean autoCommit;
    private boolean isClosed;
    private SQLWarning warningChain;

    public IginXConnection(String url, Properties info) throws SQLException, SessionException {
        if (url == null) {
            throw new IginxUrlException("Input url cannot be null");
        }
        this.params = Utils.parseUrl(url, info);

        this.session = new Session(
                params.getHost(),
                params.getPort(),
                params.getUsername(),
                params.getPassword()
        );
        this.session.openSession();

        this.isClosed = false;
        this.autoCommit = false;
        this.warningChain = null;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new IginXStatement(this, session);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLException(String.format("Statements with result set concurrency %d are not supported", resultSetConcurrency));
        }
        if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
            throw new SQLException(String.format("Statements with ResultSet type %d are not supported", resultSetType));
        }
        return new IginXStatement(this, session);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new IginXPreparedStatement(this, session, sql);
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Cannot create statement because connection is closed");
        }
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        try {
            this.session.closeSession();
        } catch (SessionException e) {
            throw new SQLException("Fail to close Statement", e);
        } finally {
            this.isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return new IginXDatabaseMetadata(this, session);
    }

    @Override
    public SQLWarning getWarnings() {
        return warningChain;
    }

    @Override
    public void clearWarnings() {
        warningChain = null;
    }

    @Override
    public boolean isValid(int timeout) {
        return !isClosed;
    }

    @Override
    public int getNetworkTimeout() {
        return Config.DEFAULT_CONNECTION_TIMEOUT_MS;
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public String getCatalog() {
        return "No catalog.";
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void rollback() {
        // do nothing in rollback
    }

    @Override
    public void rollback(Savepoint savepoint) {
        // do nothing in rollback
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new SQLClientInfoException(Constant.METHOD_NOT_SUPPORTED, null);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLClientInfoException(Constant.METHOD_NOT_SUPPORTED, null);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }
}
