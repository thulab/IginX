package cn.edu.tsinghua.iginx.parquet.entity;

import static cn.edu.tsinghua.iginx.parquet.tools.Constant.COLUMN_TIME;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.IGINX_SEPARATOR;
import static cn.edu.tsinghua.iginx.parquet.tools.Constant.PARQUET_SEPARATOR;
import static cn.edu.tsinghua.iginx.parquet.tools.DataTypeTransformer.fromParquetDataType;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;
import cn.edu.tsinghua.iginx.parquet.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetQueryRowStream implements RowStream {

    public static final Logger logger = LoggerFactory.getLogger(ParquetQueryRowStream.class);

    public static final ParquetQueryRowStream EMPTY_PARQUET_ROW_STREAM = new ParquetQueryRowStream(null, null);

    private final Header header;

    private final ResultSet rs;

    private boolean hasNext = false;

    private boolean hasNextCache = false;

    private final Map<Field, String> physicalNameCache = new HashMap<>();

    public ParquetQueryRowStream(ResultSet rs, TagFilter tagFilter) {
        this.rs = rs;

        if (rs == null) {
            this.header = new Header(Field.KEY, Collections.emptyList());
            return;
        }

        boolean filterByTags = tagFilter != null;

        Field time = null;
        List<Field> fields = new ArrayList<>();
        try {
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {  // start from index 1
                String pathName = rsMetaData.getColumnName(i).replaceAll(PARQUET_SEPARATOR, IGINX_SEPARATOR);
                if (i == 1 && pathName.equals(COLUMN_TIME)) {
                    time = Field.KEY;
                    continue;
                }

                Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(pathName);
                DataType type = fromParquetDataType(rsMetaData.getColumnTypeName(i));

                Field field  = new Field(pair.getK(), type, pair.getV());

                if (filterByTags && !TagKVUtils.match(pair.v, tagFilter)) {
                    continue;
                }
                fields.add(field);
            }
        } catch (SQLException e) {
            logger.error("encounter error when get header of result set.");
        }

        if (time == null) {
            this.header = new Header(fields);
        } else {
            this.header = new Header(Field.KEY, fields);
        }
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
            Object value = rs.getObject(getPhysicalPath(header.getField(i)));
            if (header.getField(i).getType() == DataType.BINARY && value != null) {
                values[i] = ((String) value).getBytes();
            } else {
                values[i] = value;
            }
        }
        return new Row(header, timestamp, values);
    }

    private String getPhysicalPath(Field field) {
        if (physicalNameCache.containsKey(field)) {
            return physicalNameCache.get(field);
        } else {
            String name = field.getName();
            String path = TagKVUtils.toFullName(name, field.getTags());
            path = path.replaceAll(IGINX_SEPARATOR, PARQUET_SEPARATOR);
            physicalNameCache.put(field, path);
            return path;
        }
    }
}
