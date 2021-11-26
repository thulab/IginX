package cn.edu.tsinghua.iginx.engine.shared.data;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;

import java.util.List;

public class RawData {

    private final List<String> paths;
    private final List<Long> times;
    private final Object[] values;
    private final List<DataType> types;
    private final List<Bitmap> bitmaps;
    private final RawDataType rawDataType;

    public RawData(List<String> paths, List<Long> times, Object[] values,
                   List<DataType> types, List<Bitmap> bitmaps, RawDataType rawDataType) {
        this.paths = paths;
        this.times = times;
        this.values = values;
        this.types = types;
        this.bitmaps = bitmaps;
        this.rawDataType = rawDataType;
    }

    public List<String> getPaths() {
        return paths;
    }

    public List<Long> getTimes() {
        return times;
    }

    public Object[] getValues() {
        return values;
    }

    public List<DataType> getTypes() {
        return types;
    }

    public List<Bitmap> getBitmaps() {
        return bitmaps;
    }

    public RawDataType getRawDataType() {
        return rawDataType;
    }

}
