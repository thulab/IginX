package cn.edu.tsinghua.iginx.rest.query;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryResult
{
    private List<QueryMetric> queryMetrics = new ArrayList<>();
    private List<QueryResultDataset> queryResultDatasets = new ArrayList<>();
    public int siz = 0;

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
    public void addResultSet(QueryResultDataset queryDataSet, QueryMetric queryMetric)
    {
        addqueryResultDataset(queryDataSet);
        addQueryMetric(queryMetric);
        siz += 1;
    }

    public String toResultString(int num)
    {
        StringBuilder ret = new StringBuilder("{");
        ret.append(sizeToString(num));
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
        return String.format("\"name\": \"%s\"",queryMetrics.get(num).getName());
    }

    private String groupbyToString(int num)
    {
        return "\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}]";
    }

    private String tagsToString(int num)
    {
        StringBuilder ret = new StringBuilder(" \"tags\": {");
        for (Map.Entry<String, List<String>> entry: queryMetrics.get(num).getTags().entrySet())
        {
            ret.append(String.format("\"%s\": [",entry.getKey()));
            for (String v: entry.getValue())
                ret.append(String.format("\"%s\",",v));
            ret.deleteCharAt(ret.length()-1);
            ret.append("],");
        }
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
            break;
        }
        ret.deleteCharAt(ret.length()-1);
        ret.append("]");
        return ret.toString();
    }

    private String sizeToString(int num)
    {
        StringBuilder ret = new StringBuilder("\"sample_size\": ");
        ret.append(queryResultDatasets.get(num).getSize());
        return ret.toString();
    }

}
