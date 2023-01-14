package cn.edu.tsinghua.iginx.engine.shared.data.write;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;

import java.util.List;
import java.util.Map;

public class RawData {

    private final List<String> paths;

    private final List<Map<String, String>> tagsList;

    private final List<Long> keys;

    private final Object[] valuesList;

    private final List<DataType> dataTypeList;

    private final List<Bitmap> bitmaps;

    private final RawDataType type;

    public RawData(List<String> paths, List<Map<String, String>> tagsList, List<Long> keys, Object[] valuesList,
                   List<DataType> dataTypeList, List<Bitmap> bitmaps, RawDataType type) {
        this.paths = paths;
        this.tagsList = tagsList;
        this.keys = keys;
        this.valuesList = valuesList;
        this.dataTypeList = dataTypeList;
        this.bitmaps = bitmaps;
        this.type = type;
    }

    public List<String> getPaths() {
        return paths;
    }

    public List<Map<String, String>> getTagsList() {
        return tagsList;
    }

    public List<Long> getKeys() {
        return keys;
    }

    public Object[] getValuesList() {
        return valuesList;
    }

    public List<DataType> getDataTypeList() {
        return dataTypeList;
    }

    public List<Bitmap> getBitmaps() {
        return bitmaps;
    }

    public RawDataType getType() {
        return type;
    }

    public boolean isRowData() {
        return type == RawDataType.Row || type == RawDataType.NonAlignedRow;
    }

    public boolean isColumnData() {
        return type == RawDataType.Column || type == RawDataType.NonAlignedColumn;
    }

}
