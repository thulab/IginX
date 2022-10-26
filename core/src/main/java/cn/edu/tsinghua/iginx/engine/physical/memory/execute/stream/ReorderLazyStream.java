package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ReorderLazyStream extends UnaryLazyStream {

    private final Reorder reorder;

    private Header header;

    private Map<Integer, Integer> reorderMap;

    private Row nextRow = null;

    public ReorderLazyStream(Reorder reorder, RowStream stream) {
        super(stream);
        this.reorder = reorder;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (this.header == null) {
            Header header = stream.getHeader();
            List<Field> targetFields = new ArrayList<>();
            this.reorderMap = new HashMap<>();

            for (String pattern : reorder.getPatterns()) {
                for (int i = 0; i < header.getFields().size(); i++) {
                    Field field  = header.getField(i);
                    if (!StringUtils.isPattern(pattern)) {
                        if (pattern.equals(field.getName()) || field.getName().startsWith(pattern)) {
                            reorderMap.put(targetFields.size(), i);
                            targetFields.add(field);

                        }
                    } else {
                        if (Pattern.matches(StringUtils.reformatPath(pattern), field.getName())) {
                            reorderMap.put(targetFields.size(), i);
                            targetFields.add(field);
                        }
                    }
                }
            }
            this.header = new Header(header.getTime(), targetFields);
        }
        return this.header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (nextRow == null) {
            nextRow = calculateNext();
        }
        return nextRow != null;
    }

    private Row calculateNext() throws PhysicalException {
        Header header = getHeader();
        List<Field> targetFields = header.getFields();
        if (stream.hasNext()) {
            Row row = stream.next();
            Object[] values = new Object[targetFields.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = row.getValue(reorderMap.get(i));
            }
            if (header.hasTimestamp()) {
                return new Row(header, row.getTimestamp(), values);
            } else {
                return new Row(header, values);
            }
        }
        return null;
    }


    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        Row row = nextRow;
        nextRow = null;
        return row;
    }
}
