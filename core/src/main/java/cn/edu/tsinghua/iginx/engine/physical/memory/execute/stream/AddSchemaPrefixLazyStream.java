package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSchemaPrefix;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class AddSchemaPrefixLazyStream extends UnaryLazyStream {

    private final AddSchemaPrefix addSchemaPrefix;

    private Header header;

    public AddSchemaPrefixLazyStream(AddSchemaPrefix addSchemaPrefix, RowStream stream) {
        super(stream);
        this.addSchemaPrefix = addSchemaPrefix;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (header == null) {
            Header header = stream.getHeader();
            String schemaPrefix = addSchemaPrefix.getSchemaPrefix();

            List<Field> fields = new ArrayList<>();
            header.getFields().forEach(field -> {
                if (schemaPrefix != null)
                    fields.add(new Field(schemaPrefix + "." + field.getName(), field.getType(), field.getTags()));
                else
                    fields.add(new Field(field.getName(), field.getType(), field.getTags()));
            });

            this.header = new Header(header.getKey(), fields);
        }
        return header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        return stream.hasNext();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }

        Row row = stream.next();
        if (header.hasKey()) {
            return new Row(header, row.getKey(), row.getValues());
        } else {
            return new Row(header, row.getValues());
        }
    }
}
