package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryResult
{
    private List<SessionQueryDataSet> queryDataSets = new ArrayList<>();
    private List<QueryMetric> queryMetrics = new ArrayList<>();
    public int siz = 0;

    public void setQueryMetrics(List<QueryMetric> queryMetrics)
    {
        this.queryMetrics = queryMetrics;
    }

    public void setQueryDataSets(List<SessionQueryDataSet> queryDataSets)
    {
        this.queryDataSets = queryDataSets;
    }

    public List<QueryMetric> getQueryMetrics()
    {
        return queryMetrics;
    }

    public List<SessionQueryDataSet> getQueryDataSets()
    {
        return queryDataSets;
    }

    private void addQueryMetric(QueryMetric queryMetric)
    {
        queryMetrics.add(queryMetric);
    }

    private void addqueryDataSet(SessionQueryDataSet queryDataSet)
    {
        queryDataSets.add(queryDataSet);
    }

    public void addResultSet(SessionQueryDataSet queryDataSet, QueryMetric queryMetric)
    {
        addqueryDataSet(queryDataSet);
        addQueryMetric(queryMetric);
        siz += 1;
    }

    public String toResultString(int num)
    {
        StringBuilder ret = new StringBuilder("{");
        SessionQueryDataSet queryDataSet = queryDataSets.get(num);
        QueryMetric queryMetric = queryMetrics.get(num);
        ret.append("\"sample_size\": ");
        ret.append(queryDataSet.getTimestamps().length);
        ret.append(",");
        ret.append("\"results\": [{ ");
        ret.append(String.format("\"name\": \"%s\",",queryMetric.getName()));
        ret.append("\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}],");
        ret.append(" \"tags\": {");
        ret.append(tagsToString(num));
        ret.append("},");
        ret.append(" \"values\": [");
        ret.append(valueToString(num));
        ret.append("]");
        ret.append("}]");
        ret.append("}");
        return ret.toString();
    }

    private String tagsToString(int num)
    {
        StringBuilder ret = new StringBuilder();
        for (Map.Entry<String, List<String>> entry: queryMetrics.get(num).getTags().entrySet())
        {
            ret.append(String.format("\"%s\": [",entry.getKey()));
            for (String v: entry.getValue())
                ret.append(String.format("\"%s\",",v));
            ret.deleteCharAt(ret.length()-1);
            ret.append("],");
        }
        ret.deleteCharAt(ret.length()-1);
        return ret.toString();
    }

    private String valueToString(int num)
    {
        StringBuilder ret = new StringBuilder();
        int n = queryDataSets.get(num).getTimestamps().length;
        int m = queryDataSets.get(num).getPaths().size();
        for (int i=0;i<n;i++)
        {
            ret.append(String.format("[%d,", queryDataSets.get(num).getTimestamps()[i]));
            for (int j=0;j<m;j++)
                if (queryDataSets.get(num).getValues().get(i).get(j) != null)
                {
                    if (queryDataSets.get(num).getValues().get(i).get(j) instanceof byte[])
                        ret.append(queryDataSets.get(num).getValues().get(i).get(j));
                    else
                        ret.append(queryDataSets.get(num).getValues().get(i).get(j).toString());
                    ret.append("],");
                    break;
                }
        }
        ret.deleteCharAt(ret.length()-1);
        return ret.toString();
    }

}
