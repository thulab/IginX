package cn.edu.tsinghua.iginx.rest.insert;

public class TimeAndPrefixPath {
    private long timestamp;
    private String prefixPath;
    private String key;

    public TimeAndPrefixPath(long timestamp, String prefixPath) {
        this.timestamp = timestamp;
        this.prefixPath = prefixPath;
        this.key = timestamp + prefixPath;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getPrefixPath() {
        return prefixPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TimeAndPrefixPath)) {
            return false;
        }
        TimeAndPrefixPath other = (TimeAndPrefixPath) o;
        return other.timestamp == timestamp && other.prefixPath.equals(prefixPath);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
