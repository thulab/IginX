package cn.edu.tsinghua.iginx.rest.query;


import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class QueryResult
{
    private static final IMetaManager metaManager = SortedListAbstractMetaManager.getInstance();
    private List<QueryMetric> queryMetrics = new ArrayList<>();
    private List<QueryResultDataset> queryResultDatasets = new ArrayList<>();
    private List<QueryAggregator> queryAggregators = new ArrayList<>();
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryResult.class);
    private int siz = 0;

    public void setQueryAggregators(List<QueryAggregator> queryAggregators)
    {
        this.queryAggregators = queryAggregators;
    }

    public void setSiz(int siz)
    {
        this.siz = siz;
    }

    public int getSiz()
    {
        return siz;
    }

    public List<QueryAggregator> getQueryAggregators()
    {
        return queryAggregators;
    }

    public void setQueryMetrics(List<QueryMetric> queryMetrics)
    {
        this.queryMetrics = queryMetrics;
    }

    public List<QueryMetric> getQueryMetrics()
    {
        return queryMetrics;
    }


    private void addQueryMetric(QueryMetric queryMetric)
    {
        queryMetrics.add(queryMetric);
    }

    public List<QueryResultDataset> getQueryResultDatasets()
    {
        return queryResultDatasets;
    }

    public void setQueryResultDatasets(List<QueryResultDataset> queryResultDataset)
    {
        this.queryResultDatasets = queryResultDataset;
    }

    public void addqueryResultDataset(QueryResultDataset queryResultDataset)
    {
        queryResultDatasets.add(queryResultDataset);
    }
    public void addQueryAggregator(QueryAggregator queryAggregator)
    {
        queryAggregators.add(queryAggregator);
    }

    public void addResultSet(QueryResultDataset queryDataSet, QueryMetric queryMetric, QueryAggregator queryAggregator)
    {
        addqueryResultDataset(queryDataSet);
        addQueryMetric(queryMetric);
        addQueryAggregator(queryAggregator);
        siz += 1;
    }

    public String toResultString(int num)
    {
        StringBuilder ret = new StringBuilder("{");
        ret.append(sampleSizeToString(num));
        ret.append(",");
        ret.append("\"results\": [{ ");
        ret.append(nameToString(num));
        ret.append(",");
        ret.append(groupbyToString(num));
        ret.append(",");
        ret.append(tagsToString(num));
        ret.append(",");
        ret.append(valueToString(num));
        ret.append("}]}");
        return ret.toString();
    }

    private String nameToString(int num)
    {
        if (queryAggregators.get(num).getType() == QueryAggregatorType.SAVE_AS)
        {
            return String.format("\"name\": \"%s\"",queryAggregators.get(num).getMetric_name());
        }
        else
        {
            return String.format("\"name\": \"%s\"", queryMetrics.get(num).getName());
        }
    }

    private String groupbyToString(int num)
    {
        return "\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}]";
    }

    private String tagsToString(int num)
    {
        StringBuilder ret = new StringBuilder(" \"tags\": {");
        Map<String, List<String>> tags = null;
        try
        {
            tags = getTagsFromPaths(queryMetrics.get(num).getName(),
                    queryResultDatasets.get(num).getPaths());
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during getting tags: {}", e.getMessage());
        }
        for (Map.Entry<String, List<String>> entry: tags.entrySet())
        {
            ret.append(String.format("\"%s\": [",entry.getKey()));
            for (String v: entry.getValue())
                ret.append(String.format("\"%s\",",v));
            ret.deleteCharAt(ret.length()-1);
            ret.append("],");
        }
        if (ret.charAt(ret.length()-1) == ',')
            ret.deleteCharAt(ret.length()-1);
        ret.append("}");
        return ret.toString();
    }

    private String valueToString(int num)
    {
        StringBuilder ret = new StringBuilder(" \"values\": [");
        int n = queryResultDatasets.get(num).getSize();
        for (int i=0;i<n;i++)
        {
            ret.append(String.format("[%d,", queryResultDatasets.get(num).getTimestamps().get(i)));
            if (queryResultDatasets.get(num).getValues().get(i) instanceof byte[])
                ret.append(queryResultDatasets.get(num).getValues().get(i));
            else
                ret.append(queryResultDatasets.get(num).getValues().get(i).toString());
            ret.append("],");
        }
        if (ret.charAt(ret.length()-1) == ',')
            ret.deleteCharAt(ret.length()-1);
        ret.append("]");
        return ret.toString();
    }

    private String sampleSizeToString(int num)
    {
        StringBuilder ret = new StringBuilder("\"sample_size\": ");
        ret.append(queryResultDatasets.get(num).getSampleSize());
        return ret.toString();
    }

    private Map<String, List<String>> getTagsFromPaths(String name, List<String> paths) throws Exception
    {
        List<Map<String, Integer>> dup = new ArrayList<>();
        Map<String, List<String>> ret = new TreeMap<>();
        Map<Integer, String> pos2path = new TreeMap<>();
        Map<String, Integer> metricschema = metaManager.getSchemaMapping(name);
        if (metricschema == null)
        {
            throw new Exception("No metadata found");
        }
        else
        {
            for (Map.Entry<String, Integer> entry: metricschema.entrySet())
            {
                pos2path.put(entry.getValue(), entry.getKey());
                dup.add(new HashMap<>());
            }
        }
        for (String path: paths)
        {
            String[] splitpaths = path.split("\\.");
            for (int i=0;i < pos2path.size();i++)
            {
                if (dup.get(i).get(splitpaths[i]) == null)
                {
                    dup.get(i).put(splitpaths[i], 1);
                    if (ret.get(pos2path.get(i+1)) == null)
                    {
                        ret.put(pos2path.get(i+1), new ArrayList<>());
                    }
                    ret.get(pos2path.get(i+1)).add(splitpaths[i]);
                }
            }

        }
        return ret;
    }
}
