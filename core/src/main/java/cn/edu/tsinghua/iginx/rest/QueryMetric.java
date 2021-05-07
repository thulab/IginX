package cn.edu.tsinghua.iginx.rest;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class QueryMetric
{
    private String name;
    private Long limit;
    private Map<String, List<String>> tags = new TreeMap();
    private List<QueryAggregator> aggregators = new ArrayList<>();


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Map<String, List<String>> getTags() {
        return tags;
    }

    public void setTags(Map<String, List<String>> tags) {
        this.tags = tags;
    }

    public List<QueryAggregator> getAggregators() {
        return aggregators;
    }

    public void setAggregators(
            List<QueryAggregator> aggregators) {
        this.aggregators = aggregators;
    }

    public void addTag(String key, String value)
    {
        if (tags.get(key) == null)
            tags.put(key, new ArrayList<>());
        tags.get(key).add(value);
    }
    public void addAggregator(QueryAggregator qa)
    {
        aggregators.add(qa);
    }
}
