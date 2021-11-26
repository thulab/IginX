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
        return endPathIndex - startPathIndex;
    }

    public int getTypesSize() {
        return endPathIndex - startPathIndex;
    }

    public int getTimesSize() {
        return endTimeIndex - startTimeIndex;
    }

    public String getPath(int pathIndex) {
        checkPathIndexRange(pathIndex);
        return data.getPaths().get(startPathIndex + pathIndex);
    }


    public DataType getType(int typeIndex) {
        checkTypeIndexRange(typeIndex);
        return data.getTypes().get(startPathIndex + typeIndex);
    }

    public Long getTime(int timeIndex) {
        checkTimeIndexRange(timeIndex);
        return data.getTimes().get(startTimeIndex + timeIndex);
    }

    public Object getValue(int pathIndex, int timeIndex) {
        checkPathIndexRange(pathIndex);
        checkTimeIndexRange(timeIndex);

        RawDataType type = data.getRawDataType();
        int timeOffset = startTimeIndex + timeIndex;
        int pathOffset = startPathIndex + pathIndex;

        if (type == RawDataType.Column || type == RawDataType.NonAlignedColumn) {
            Object colData = data.getValues()[pathOffset];
            if (data.getBitmaps().get(pathOffset).get(timeOffset)) {
                // TODO @zy
                return null;
            } else {
                return null;
            }
        } else {
            Object rowData = data.getValues()[timeOffset];
            if (data.getBitmaps().get(timeOffset).get(pathOffset)) {
                // TODO @zy
                return null;
            } else {
                return null;
            }
        }
    }

    private void checkPathIndexRange(int pathIndex) {
        if (pathIndex < 0 || pathIndex >= endPathIndex - startPathIndex)
            throw new IllegalArgumentException(String.format("path index out of range [%d, %d)", 0, endPathIndex - startPathIndex));
    }

    // startPathIndex equals to startTypeIndex, endPathIndex equals to endTypeIndex
    private void checkTypeIndexRange(int typeIndex) {
        if (typeIndex < 0 || typeIndex >= endPathIndex - startPathIndex)
            throw new IllegalArgumentException(String.format("type index out of range [%d, %d)", 0, endPathIndex - startPathIndex));
    }

    private void checkTimeIndexRange(int timeIndex) {
        if (timeIndex < 0 || timeIndex >= endTimeIndex - startTimeIndex)
            throw new IllegalArgumentException(String.format("time index out of range [%d, %d)", 0, endTimeIndex - startTimeIndex));
    }
}
