package cn.edu.tsinghua.iginx.engine.shared.data;

import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.List;

public class DataSection {

    private final RawData data;

    private final int startPath;
    private final int endPath;
    private final int startTime;
    private final int endTime;

    public DataSection(RawData data, int startPath, int endPath, int startTime, int endTime) {
        this.data = data;
        this.startPath = startPath;
        this.endPath = endPath;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public List<String> getPaths() {
        return data.getPaths().subList(startPath, endPath);
    }

    public List<DataType> getTypes() {
        return data.getTypes().subList(startPath, endPath);
    }

    public List<Long> getTimes() {
        return data.getTimes().subList(startTime, endTime);
    }

    public Object getValue(int pathIndex, int timeIndex) {
        if (pathIndex < startPath || pathIndex >= endPath)
            throw new IllegalArgumentException("");
        if (timeIndex < startTime || timeIndex >= endTime)
            throw new IllegalArgumentException("");

        return data.getValues()[pathIndex][timeIndex];
    }
}
