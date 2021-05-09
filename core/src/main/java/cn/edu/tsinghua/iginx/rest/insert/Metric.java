package cn.edu.tsinghua.iginx.rest.insert;

import java.util.*;

public class Metric
{
    private String name;
    private Map<String, String> tags = new TreeMap<>();
    private List<Long> timestamps = new ArrayList<>();
    private List<String> values = new ArrayList<>();

    public String getName()
    {
        return name;
    }

    public List<Long> getTimestamps()
    {
        return timestamps;
    }

    public List<String> getValues()
    {
        return values;
    }

    public Map<String, String> getTags()
    {
        return tags;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public void setTags(Map<String, String> tags)
    {
        this.tags = tags;
    }

    public void setTimestamps(List<Long> timestamps)
    {
        this.timestamps = timestamps;
    }

    public void setValues(List<String> values)
    {
        this.values = values;
    }

    public void addTag(String key, String value)
    {
        tags.put(key, value);
    }

    public void addTimestamp(Long timestamp)
    {
        timestamps.add(timestamp);
    }

    public void addValue(String value)
    {
        values.add(value);
    }
}
