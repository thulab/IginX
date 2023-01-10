package cn.edu.tsinghua.iginx.parquet.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.HashMap;
import java.util.Map;

public class DataViewWrapper {

    private final DataView dataView;

    private final Map<Integer, String> pathCache;

    public DataViewWrapper(DataView dataView) {
        this.dataView = dataView;
        this.pathCache = new HashMap<>();
    }

    public int getPathNum() {
        return dataView.getPathNum();
    }

    public int getTimeSize() {
        return dataView.getTimeSize();
    }

    public String getPath(int index) {
        if (pathCache.containsKey(index)) {
            return pathCache.get(index);
        }
        String path = dataView.getPath(index);
        Map<String, String> tags = dataView.getTags(index);
        path = TagKVUtils.toFullName(path, tags);
        pathCache.put(index, path);
        return path;
    }

    public DataType getDataType(int index) {
        return dataView.getDataType(index);
    }

    public Long getTimestamp(int index) {
        return dataView.getKey(index);
    }

    public Object getValue(int index1, int index2) {
        return dataView.getValue(index1, index2);
    }

    public BitmapView getBitmapView(int index) {
        return dataView.getBitmapView(index);
    }

    public RawDataType getRawDataType() {
        return dataView.getRawDataType();
    }

    public int getPathIndex(String path) {
        for (int i = 0; i < getPathNum(); i++) {
            if (getPath(i).equals(path)) {
                return i;
            }
        }
        return -1;
    }

    public int getTimestampIndex(long timestamp) {
        return dataView.getKeyIndex(timestamp);
    }
}
