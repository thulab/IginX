package cn.edu.tsinghua.iginx.engine.shared.data;

import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.List;

public class DataSection {

    private final RawData data;

    private final int startPathIndex;
    private final int endPathIndex;
    private final int startTimeIndex;
    private final int endTimeIndex;

    public DataSection(RawData data, int startPathIndex, int endPathIndex, int startTimeIndex, int endTimeIndex) {
        this.data = data;
        this.startPathIndex = startPathIndex;
        this.endPathIndex = endPathIndex;
        this.startTimeIndex = startTimeIndex;
        this.endTimeIndex = endTimeIndex;
    }

    public List<String> getPaths() {
        return data.getPaths().subList(startPathIndex, endPathIndex);
    }

    public List<DataType> getTypes() {
        return data.getTypes().subList(startPathIndex, endPathIndex);
    }

    public List<Long> getTimes() {
        return data.getTimes().subList(startTimeIndex, endTimeIndex);
    }

    public int getPathsSize() {
        return data.getPaths().subList(startPathIndex, endPathIndex).size();
    }

    public int getTypesSize() {
        return data.getTypes().subList(startPathIndex, endPathIndex).size();
    }

    public int getTimesSize() {
        return data.getTimes().subList(startTimeIndex, endTimeIndex).size();
    }

    public String getPath(int pathIndex) {
        return data.getPaths().subList(startPathIndex, endPathIndex).get(pathIndex);
    }

    public DataType getType(int typeIndex) {
        return data.getTypes().subList(startPathIndex, endPathIndex).get(typeIndex);
    }

    public Long getTime(int timeIndex) {
        return data.getTimes().subList(startTimeIndex, endTimeIndex).get(timeIndex);
    }

    public Object getValue(int pathIndex, int timeIndex) {
        if (pathIndex < 0 || pathIndex >= endPathIndex - startPathIndex)
            throw new IllegalArgumentException(String.format("path index out of range [%d, %d)", 0, endPathIndex - startPathIndex));
        if (timeIndex < 0 || timeIndex >= endTimeIndex - startTimeIndex)
            throw new IllegalArgumentException(String.format("time index out of range [%d, %d)", 0, endTimeIndex - startTimeIndex));

        return data.getValues()[startPathIndex + pathIndex][startTimeIndex + timeIndex];
    }
}
