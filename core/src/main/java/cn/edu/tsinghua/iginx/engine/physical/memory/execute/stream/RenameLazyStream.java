package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Rename;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class RenameLazyStream extends UnaryLazyStream {

    private final Rename rename;

    private Header header;

    public RenameLazyStream(Rename rename, RowStream stream) {
        super(stream);
        this.rename = rename;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (header == null) {
            Header header = stream.getHeader();
            Map<String, String> aliasMap = rename.getAliasMap();

            List<Field> fields = new ArrayList<>();
            header.getFields().forEach(field -> {
                String alias = "";
                for (String oldName : aliasMap.keySet()) {
                    Pattern pattern = Pattern.compile(StringUtils.reformatColumnName(oldName) + ".*");
                    if (pattern.matcher(field.getFullName()).matches()) {
                        alias = aliasMap.get(oldName);
                        break;
                    }
                }
                if (alias.equals("")) {
                    fields.add(field);
                } else {
                    fields.add(new Field(alias, field.getType(), field.getTags()));
                }
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
