package cn.edu.tsinghua.iginx.jdbc;

import cn.edu.tsinghua.iginx.session.Session;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class IginXPreparedStatement extends IginXStatement implements PreparedStatement {

    private String sql;
    private final Map<Integer, String> params = new LinkedHashMap<>();

    public IginXPreparedStatement(IginXConnection connection, Session session, String sql) {
        super(connection, session);
        this.sql = sql;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return super.executeQuery(createCompleteSql(sql, params));
    }

    @Override
    public int executeUpdate() throws SQLException {
        return super.executeUpdate(createCompleteSql(sql, params));
    }

    @Override
    public boolean execute() throws SQLException {
        return super.execute(createCompleteSql(sql, params));
    }

    // Only for tests.
    public String getCompleteSql() {
        try {
            return createCompleteSql(sql, params);
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        }
    }

    private String createCompleteSql(final String sql, Map<Integer, String> parameters)
            throws SQLException {
        List<String> parts = splitSqlStatement(sql);

        StringBuilder newSql = new StringBuilder(parts.get(0));
        for (int i = 1; i < parts.size(); i++) {
            if (!parameters.containsKey(i)) {
                throw new SQLException("Parameter #" + i + " is unset");
            }
            newSql.append(parameters.get(i));
            newSql.append(parts.get(i));
        }
        return newSql.toString();
    }

    private List<String> splitSqlStatement(final String sql) {
        List<String> parts = new ArrayList<>();
        int apCount = 0;
        int off = 0;
        boolean skip = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (skip) {
                skip = false;
                continue;
            }
            switch (c) {
                case '\'':
                case '\"':
                    // skip something like 'xxxxx' & "xxxxx"
                    apCount++;
                    break;
                case '\\':
                    // skip something like \r\n
                    skip = true;
                    break;
                case '?':
                    // for input like: select a from 'bc' where d, 'bc' will be skipped
                    if ((apCount & 1) == 0) {
                        parts.add(sql.substring(off, i));
                        off = i + 1;
                    }
                    break;
                default:
                    break;
            }
        }
        parts.add(sql.substring(off));
        return parts;
    }

    @Override
    public void clearParameters() {
        params.clear();
    }

    @Override
    public ParameterMetaData getParameterMetaData() {
        return new ParameterMetaData() {
            @Override
            public int getParameterCount() {
                return params.size();
            }

            @Override
            public int isNullable(int param) {
                return ParameterMetaData.parameterNullableUnknown;
            }

            @Override
            public boolean isSigned(int param) {
                try {
                    return Integer.parseInt(params.get(param)) < 0;
                } catch (Exception e) {
                    return false;
                }
            }

            @Override
            public int getPrecision(int param) {
                return params.get(param).length();
            }

            @Override
            public int getScale(int param) {
                try {
                    double d = Double.parseDouble(params.get(param));
                    if (d >= 1) { // we only need the fraction digits
                        d = d - (long) d;
                    }
                    if (d == 0) { // nothing to count
                        return 0;
                    }
                    d *= 10; // shifts 1 digit to left
                    int count = 1;
                    while (d - (long) d != 0) { // keeps shifting until there are no more fractions
                        d *= 10;
                        count++;
                    }
                    return count;
                } catch (Exception e) {
                    return 0;
                }
            }

            @Override
            public int getParameterType(int param) {
                return 0;
            }

            @Override
            public String getParameterTypeName(int param) {
                return null;
            }

            @Override
            public String getParameterClassName(int param) {
                return null;
            }

            @Override
            public int getParameterMode(int param) {
                return 0;
            }

            @Override
            public <T> T unwrap(Class<T> iface) {
                return null;
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) {
                return false;
            }
        };
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return super.getResultSet().getMetaData();
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        params.put(parameterIndex, Boolean.toString(x));
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        params.put(parameterIndex, Integer.toString(x));
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        params.put(parameterIndex, Long.toString(x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        params.put(parameterIndex, Float.toString(x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        params.put(parameterIndex, Double.toString(x));
    }

    @Override
    public void setString(int parameterIndex, String x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else {
            throw new SQLException(
                    String.format(
                            "Can''t infer the SQL type to use for an instance of %s. Use setObject() with"
                                    + " an explicit Types value to specify the type to use.",
                            x.getClass().getName()));
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }
}
