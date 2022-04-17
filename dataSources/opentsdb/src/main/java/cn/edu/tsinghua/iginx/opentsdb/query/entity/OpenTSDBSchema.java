package cn.edu.tsinghua.iginx.opentsdb.query.entity;

import java.util.HashMap;
import java.util.Map;

public class OpenTSDBSchema {

    private static final String STORAGE_UNIT = "du";

    private final String metric;

    private final Map<String, String> tags = new HashMap<>();

    public OpenTSDBSchema(String path, String storageUnit) {
        this.metric = storageUnit + "." + path;
        this.tags.put(STORAGE_UNIT, storageUnit);
    }

    public String getMetric() {
        return metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "OpenTSDBSchema{" +
                "measurement='" + metric + '\'' +
                ", tags=" + tags +
                '}';
    }
}
