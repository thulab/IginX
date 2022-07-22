package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.data.write.RawData;
import cn.edu.tsinghua.iginx.engine.shared.data.write.RawDataType;
import cn.edu.tsinghua.iginx.sql.SQLConstant;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;

import java.util.*;

public class InsertStatement extends DataStatement {

    private final RawDataType rawDataType;

    private String prefixPath;
    private List<String> paths;
    private Map<String, String> globalTags;
    private List<Map<String, String>> tagsList;
    private List<Long> times;
    private Object[] values;
    private List<DataType> types;
    private List<Bitmap> bitmaps;

    public InsertStatement(RawDataType rawDataType) {
        this.statementType = StatementType.INSERT;
        this.rawDataType = rawDataType;
        this.paths = new ArrayList<>();
        this.types = new ArrayList<>();
        this.bitmaps = new ArrayList<>();
        this.tagsList = new ArrayList<>();
    }

    public InsertStatement(RawDataType rawDataType, List<String> paths, List<Long> times,
                           Object[] values, List<DataType> types, List<Bitmap> bitmaps, List<Map<String, String>> tagsList) {
        this.statementType = StatementType.INSERT;
        this.rawDataType = rawDataType;
        this.paths = paths;
        this.times = times;
        this.values = values;
        this.types = types;
        this.bitmaps = bitmaps;
        this.tagsList = tagsList;
    }

    public String getPrefixPath() {
        return prefixPath;
    }

    public void setPrefixPath(String prefixPath) {
        this.prefixPath = prefixPath;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public void setPath(String path) {
        setPath(path, null);
    }

    public void setPath(String path, Map<String, String> tags) {
        this.paths.add(prefixPath + SQLConstant.DOT + path);
        this.tagsList.add(tags);
    }

    public Map<String, String> getGlobalTags() {
        return globalTags;
    }

    public void setGlobalTags(Map<String, String> globalTags) {
        this.globalTags = globalTags;
    }

    public boolean hasGlobalTags() {
        return this.globalTags != null;
    }

    public List<Map<String, String>> getTagsList() {
        return tagsList;
    }

    public void setTagsList(List<Map<String, String>> tagsList) {
        this.tagsList = tagsList;
    }

    public List<Long> getTimes() {
        return times;
    }

    public void setTimes(List<Long> times) {
        this.times = times;
    }

    public Object[] getValues() {
        return values;
    }

    public void setValues(Object[][] values) {
        this.values = values;
    }

    public List<DataType> getTypes() {
        return types;
    }

    public void setTypes(List<DataType> types) {
        this.types = types;
    }

    public List<Bitmap> getBitmaps() {
        return bitmaps;
    }

    public void setBitmaps(List<Bitmap> bitmaps) {
        this.bitmaps = bitmaps;
    }

    public long getStartTime() {
        return times.get(0);
    }

    public long getEndTime() {
        return times.get(times.size() - 1);
    }

    public void sortData() {
        Integer[] index = new Integer[times.size()];
        for (int i = 0; i < times.size(); i++) {
            index[i] = i;
        }
        Arrays.sort(index, Comparator.comparingLong(times::get));
        Collections.sort(times);
        for (int i = 0; i < values.length; i++) {
            Object[] tmpValues = new Object[index.length];
            for (int j = 0; j < index.length; j++) {
                tmpValues[j] = ((Object[]) values[i])[index[j]];
            }
            values[i] = tmpValues;
        }

        index = new Integer[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            index[i] = i;
        }
        Arrays.sort(index, Comparator.comparing(paths::get));
        Collections.sort(paths);
        Object[] sortedValuesList = new Object[values.length];
        List<DataType> sortedDataTypeList = new ArrayList<>();
        List<Map<String, String>> sortedTagsList = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            sortedValuesList[i] = values[index[i]];
            sortedDataTypeList.add(types.get(index[i]));
            if (!hasGlobalTags()) {
                sortedTagsList.add(tagsList.get(index[i]));
            }
        }

        for (int i = 0; i < sortedValuesList.length; i++) {
            Object[] values = (Object[]) sortedValuesList[i];
            Bitmap bitmap = new Bitmap(times.size());
            for (int j = 0; j < times.size(); j++) {
                if (values[j] != null) {
                    bitmap.mark(j);
                }
            }
            bitmaps.add(bitmap);
        }

        values = sortedValuesList;
        types = sortedDataTypeList;
        tagsList = sortedTagsList;
    }

    public RawData getRawData() {
        List<Map<String, String>> tagsList = this.tagsList;
        if (globalTags != null) {
            for (int i = 0; i < paths.size(); i++) {
                tagsList.add(globalTags);
            }
        }
        return new RawData(paths, tagsList, times, values, types, bitmaps, rawDataType);
    }
}
