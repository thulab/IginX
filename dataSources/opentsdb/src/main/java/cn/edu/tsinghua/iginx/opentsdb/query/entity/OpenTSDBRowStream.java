package cn.edu.tsinghua.iginx.opentsdb.query.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.opentsdb.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.opentsdb.client.bean.response.QueryResult;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.opentsdb.tools.DataTypeTransformer.*;

public class OpenTSDBRowStream implements RowStream {

    private static final String STORAGE_UNIT = "du";

    private final Header header;

    private final List<QueryResult> resultList;

    private final Iterator<Map.Entry<Long, Number>>[] iterators;

    private final Map.Entry<Long, Number>[] curData;

    private final boolean[] finished;

    private final boolean trimStorageUnit;

    private int hasMoreRecords;

    public OpenTSDBRowStream(List<QueryResult> resultList, boolean trimStorageUnit) {
        this.resultList = resultList;
        this.trimStorageUnit = trimStorageUnit;

        List<Field> fields = new ArrayList<>();
        for (QueryResult res : resultList) {
            String metric = res.getMetric();
            String path = metric;
            if (trimStorageUnit && res.getTags().containsKey(STORAGE_UNIT)) {
                path = metric.substring(metric.indexOf(".") + 1);
            }
            DataType dataType = fromOpenTSDB(res.getTags().get(DATA_TYPE));
            Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(path);
            fields.add(new Field(pair.getK(), dataType, pair.getV()));
        }
        this.header = new Header(Field.KEY, fields);

        this.iterators = new Iterator[this.resultList.size()];
        this.curData = new Map.Entry[this.resultList.size()];
        this.finished = new boolean[this.resultList.size()];
        for (int i = 0; i < resultList.size(); i++) {
            this.iterators[i] = resultList.get(i).getDps().entrySet().iterator();
        }
        this.hasMoreRecords = this.resultList.size();
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public void close() throws PhysicalException {
        // need to do nothing
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        return this.hasMoreRecords > 0;
    }

    @Override
    public Row next() throws PhysicalException {
        long timestamp = Long.MAX_VALUE;
        for (int i = 0; i < this.resultList.size(); i++) {
            Iterator<Map.Entry<Long, Number>> iterator = this.iterators[i];
            if (!iterator.hasNext() && curData[i] == null) {  // 数据已经消费完毕了
                continue;
            }
            if (curData[i] == null) {
                Map.Entry<Long, Number> entry = iterator.next();
                timestamp = Math.min(timestamp, entry.getKey());
                curData[i] = entry;
            }
        }
        if (timestamp == Long.MAX_VALUE) {
            return null;
        }
        Object[] values = new Object[this.resultList.size()];
        for (int i = 0; i < this.resultList.size(); i++) {
            Iterator<Map.Entry<Long, Number>> iterator = this.iterators[i];
            if (!iterator.hasNext() && curData[i] == null) {  // 数据已经消费完毕了
                continue;
            }
            if (curData[i].getKey() == timestamp) {
                values[i] = getValue(resultList.get(i).getTags().get(DATA_TYPE), curData[i].getValue());
                curData[i] = null;
            }
            if (!iterator.hasNext() && curData[i] == null) {  // 数据已经消费完毕了
                if (!finished[i]) {
                    finished[i] = true;
                    hasMoreRecords--;
                }
            }
        }
        return new Row(header, timestamp, values);
    }
}
