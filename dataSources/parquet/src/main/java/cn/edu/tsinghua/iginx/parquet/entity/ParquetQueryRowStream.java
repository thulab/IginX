package cn.edu.tsinghua.iginx.parquet.entity;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.COLUMN_TIME;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.IGINX_SEPARATOR;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.PARQUET_SEPARATOR;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

public class ParquetQueryRowStream implements RowStream {

    public static final ParquetQueryRowStream EMPTY_PARQUET_ROW_STREAM =
        new ParquetQueryRowStream(new Header(Field.TIME, Collections.emptyList()), null);

    private final Header header;

    private final ResultSet rs;

    private boolean hasNext = false;

    private boolean hasNextCache = false;

    public ParquetQueryRowStream(Header header, ResultSet rs) {
        this.header = header;
        this.rs = rs;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        return header;
    }

    @Override
    public void close() throws PhysicalException {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            throw new PhysicalException(e);
        }
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        // DuckDB JDBC does not implements many function
        // so we need a tricky way to maintains the idempotency of hasNext()
        if (rs == null) {
            return false;
        } else if (hasNextCache) {
            return hasNext;
        } else {
            try {
                hasNext = rs.next();
                hasNextCache = true;
                return hasNext;
            } catch (SQLException e) {
                throw new PhysicalException(e);
            }
        }
    }

    @Override
    public Row next() throws PhysicalException {
        try {
            if (rs == null) {
                return null;
            } else if (hasNextCache) {
                hasNextCache = false;
                return constructOneRow();
            } else if (rs.next()) {
                return constructOneRow();
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new PhysicalException(e);
        }
    }

    private Row constructOneRow() throws SQLException {
        long timestamp = (long) rs.getObject(COLUMN_TIME);
        Object[] values = new Object[header.getFieldSize()];
        for (int i = 0; i < header.getFieldSize(); i++) {
            Object value = rs.getObject(header.getField(i).getName().replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR));
            if (header.getField(i).getType() == DataType.BINARY && value != null) {
                values[i] = ((String) value).getBytes();
            } else {
                values[i] = value;
            }
        }
        return new Row(header, timestamp, values);
    }
}