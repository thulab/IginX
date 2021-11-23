package cn.edu.tsinghua.iginx.engine.shared.data;

import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.List;

public class RawData {

    private final List<String> paths;
    private final List<Long> times;
    private final Object[][] values;
    private final List<DataType> types;

    public RawData(List<String> paths, List<Long> times, Object[][] values, List<DataType> types) {
        this.paths = paths;
        this.times = times;
        this.values = values;
        this.types = types;
    }

    public List<String> getPaths() {
        return paths;
    }

    public List<Long> getTimes() {
        return times;
    }

    public Object[][] getValues() {
        return values;
    }

    public List<DataType> getTypes() {
        return types;
    }
}
