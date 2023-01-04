package cn.edu.tsinghua.iginx.jdbc;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
//import com.google.common.primitives.Ints;
//import com.google.common.primitives.Longs;
//import com.google.common.primitives.Shorts;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

@Slf4j
public class IginXResultSet implements ResultSet {

    private final Statement statement;
    private final ResultSetMetaData metadata;

    private List<String> columnNames;
    private List<DataType> columnTypes;
    private List<List<Object>> values;

    private int pos;
    private int fetchSize;
    private boolean isClosed;
    private boolean wasNull;
    private boolean hasTime;
    private SQLWarning warningChain;

    public IginXResultSet(Statement statement, SessionExecuteSqlResult result) {
        switch (result.getSqlType()) {
            case Insert:
            case Delete:
            case AddStorageEngines:
                break;
            case GetReplicaNum:
                constructReplicaNumResult(result);
                break;
            case Query:
                constructQueryResult(result);
        }

        this.pos = -1;
        this.isClosed = false;
        this.statement = statement;
        this.metadata = new IginXResultMetadata(columnNames, columnTypes, hasTime);
    }

    private void constructQueryResult(SessionExecuteSqlResult result) {
        columnTypes = result.getDataTypeList();
        values = result.getValues();

        columnNames = result.getPaths();

        long[] keys = result.getKeys();
        if (keys != null) {
            if (keys.length != values.size()) {
                log.error("keys and values size did not match.");
                return;
            }
            columnNames.add(0, GlobalConstant.KEY_NAME);
            columnTypes.add(0, DataType.LONG);
            for (int i = 0; i < keys.length; i++) {
                values.get(i).add(0, keys[i]);
            }
            hasTime = true;
        } else {
            hasTime = false;
        }
    }

    private void constructReplicaNumResult(SessionExecuteSqlResult result) {
        columnTypes = new ArrayList<>();
        columnTypes.add(DataType.BINARY);

        columnNames = new ArrayList<>();
        columnNames.add("replica_num");

        values = new ArrayList<>();
        List<Object> rowValues = new ArrayList<>();
        rowValues.add(result.getReplicaNum());
        values.add(rowValues);
    }

    private void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("IginXResultSet has been closed!");
        }
    }

    private void checkAvailability(int columnIndex, int bounds) throws SQLException {
        checkClosed();
        if (columnIndex < 1)
            throw new SQLException("Column Index out of range, " + columnIndex + " < 1");
        if (columnIndex > bounds)
            throw new SQLException("Column Index out of range, " + columnIndex + " > " + bounds);
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        pos++;
        return pos < values.size();
    }

    @Override
    public void close() {
        synchronized (IginXResultSet.class) {
            isClosed = true;
        }
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        if (value instanceof byte[])
            return new String((byte[]) value);
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null) {
            wasNull = true;
            return false;
        }
        wasNull = false;
        if (value instanceof Boolean)
            return (boolean) value;
        return Boolean.parseBoolean(value.toString());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Byte.MIN_VALUE)
            return 0;
        if (valueAsLong < Byte.MIN_VALUE || valueAsLong > Byte.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.TINYINT);

        return (byte) valueAsLong;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Short.MIN_VALUE)
            return 0;
        if (valueAsLong < Short.MIN_VALUE || valueAsLong > Short.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.SMALLINT);
        return (short) valueAsLong;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        long valueAsLong = Long.parseLong(value.toString());
        if (valueAsLong == Integer.MIN_VALUE)
            return 0;
        if (valueAsLong < Integer.MIN_VALUE || valueAsLong > Integer.MAX_VALUE)
            throwRangeException(value.toString(), columnIndex, Types.INTEGER);
        return (int) valueAsLong;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        long valueAsLong = 0;
        try {
            valueAsLong = Long.parseLong(value.toString());
            if (valueAsLong == Long.MIN_VALUE)
                return 0;
        } catch (NumberFormatException e) {
            throwRangeException(value.toString(), columnIndex, Types.BIGINT);
        }
        return valueAsLong;
    }

    private void throwRangeException(String valueAsString, int columnIndex, int jdbcType) throws SQLException {
        throw new SQLException("Numeric value out of range" +
            "'" + valueAsString + "' in column '" + columnIndex +
            "' is outside valid range for the jdbcType " + jdbcType);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Float)
            return (float) value;
        if (value instanceof Double)
            return new Float((Double) value);
        return Float.parseFloat(value.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if (value instanceof Double || value instanceof Float)
            return (double) value;
        return Double.parseDouble(value.toString());
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof byte[])
            return (byte[]) value;
        if (value instanceof String)
            return ((String) value).getBytes();
        if (value instanceof Long)
            return Utils.LongToByteArray((long) value);
        if (value instanceof Integer)
            return Utils.IntToByteArray((int) value);
        if (value instanceof Short)
            return Utils.ShortToByteArray((short) value);
        if (value instanceof Byte)
            return new byte[]{(byte) value};
        if (value instanceof Timestamp) {
            return Utils.formatTimestamp((Timestamp) value).getBytes();
        }

        return value.toString().getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return new Date(((Timestamp) value).getTime());
        return Utils.parseDate(value.toString());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return new Time(((Timestamp) value).getTime());
        Time time = null;
        try {
            time = Utils.parseTime(value.toString());
        } catch (DateTimeParseException ignored) {
        }
        return time;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;
        if (value instanceof Timestamp)
            return (Timestamp) value;
        if (value instanceof Long) {
            if (1_0000_0000_0000_0L > (long) value)
                return Timestamp.from(Instant.ofEpochMilli((long) value));
            long epochSec = (long) value / 1000_000L;
            long nanoAdjustment = (long) value % 1000_000L * 1000;
            return Timestamp.from(Instant.ofEpochSecond(epochSec, nanoAdjustment));
        }
        Timestamp ret;
        try {
            ret = Utils.parseTimestamp(value.toString());
        } catch (Exception e) {
            ret = null;
        }
        return ret;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());

        Object value = values.get(pos).get(columnIndex - 1);
        if (value == null)
            return null;

        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
            return new BigDecimal(Long.parseLong(value.toString()));
        if (value instanceof Double || value instanceof Float)
            return BigDecimal.valueOf(Double.parseDouble(value.toString()));
        if (value instanceof Timestamp)
            return new BigDecimal(((Timestamp) value).getTime());

        BigDecimal ret;
        try {
            ret = new BigDecimal(value.toString());
        } catch (Exception e) {
            ret = null;
        }
        return ret;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkAvailability(columnIndex, values.get(pos).size());
        return values.get(pos).get(columnIndex - 1);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
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
    public String getCursorName() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return metadata;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        int columnIndex = columnNames.indexOf(columnLabel);
        if (columnIndex == -1)
            throw new SQLException("cannot find Column in resultSet");
        return columnIndex + 1;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return pos == -1 && values.size() != 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return pos >= values.size() && values.size() != 0;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return pos == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        if (values.size() == 0)
            return false;
        return pos == values.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        synchronized (this) {
            if (values.size() > 0) {
                this.pos = -1;
            }
        }
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        synchronized (this) {
            if (values.size() > 0) {
                this.pos = values.size();
            }
        }
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (values.size() == 0)
            return false;
        synchronized (this) {
            pos = 0;
        }
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        if (values.size() == 0)
            return false;
        synchronized (this) {
            pos = values.size() - 1;
        }
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        int row;
        synchronized (this) {
            if (pos < 0 || pos >= values.size())
                return 0;
            row = pos + 1;
        }
        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0)
            throw new SQLException("fetch size should >= 0");
        this.fetchSize = rows;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLException(Constant.METHOD_NOT_SUPPORTED);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
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
