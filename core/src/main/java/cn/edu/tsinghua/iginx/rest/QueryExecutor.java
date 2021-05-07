package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class QueryExecutor
{
    private final IMetaManager metaManager = SortedListAbstractMetaManager.getInstance();
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();

    private Query query;

    private Long startTime;
    private Long endTime;

    private Map<String, Integer> tag2pos;
    private Session session = new Session("127.0.0.1", 6324, "root", "root");
    private Map<String, Map<String,Integer>> schemamapping = MetricsResource.schemamapping;

    private String[] sortedTagKeys; //将指定的key和未指定的key（用*代替）按位置排序

    private Map<Integer, List<String>> tmpTags;

    public QueryExecutor(Query query)
    {
        this.query = query;
    }

    public QueryResult execute()
    {
        QueryResult ret = new QueryResult();
        try
        {
            session.openSession();
        }
        catch (SessionException e)
        {
            e.printStackTrace();
        }
        for (QueryMetric queryMetric : query.getQueryMetrics())
        {
            List<String> paths = getPaths(queryMetric);
            if (queryMetric.getAggregators().size() == 0)
            {
                try
                {
                    ret.addResultSet(session.queryData(paths, query.getStartAbsolute(), query.getEndAbsolute()), queryMetric);
                }
                catch (SessionException e)
                {
                    e.printStackTrace();
                }
            }
            else
            {
                for (QueryAggregator queryAggregator: queryMetric.getAggregators())
                {
                    try
                    {
                        ret.addResultSet(session.downsampleQuery(paths, query.getStartAbsolute(), query.getEndAbsolute(),
                                queryAggregator.getAggregateType(), queryAggregator.getDur()), queryMetric);
                    }
                    catch (SessionException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
        return ret;
    }

    public List<String> getPaths(QueryMetric queryMetric)
    {
        List<String> ret = new ArrayList<>();
        Map<String, Integer> metricschema = schemamapping.get(queryMetric.getName());
        if (metricschema == null)
        {

        }
        else
        {
            Map<Integer, String> pos2path = new TreeMap<>();
            for (Map.Entry<String, Integer> entry: metricschema.entrySet())
                pos2path.put(entry.getValue(), entry.getKey());
            List<Integer> pos = new ArrayList<>();
            for (int i=0;i<pos2path.size();i++)
                pos.add(0);
            dfsInsert(0, ret, pos2path, queryMetric, pos);
        }
        return ret;
    }

    void dfsInsert(int depth, List<String> Paths, Map<Integer, String> pos2path, QueryMetric queryMetric, List<Integer> pos)
    {
        if (depth == pos2path.size())
        {
            StringBuilder path = new StringBuilder("");
            Iterator iter = pos2path.entrySet().iterator();
            int now = 0;
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry) iter.next();
                String ins = queryMetric.getTags().get(entry.getValue()).get(pos.get(now));
                if (ins != null)
                    path.append(ins + ".");
                else
                    path.append("*.");
                now++;
            }
            path.append(queryMetric.getName());
            Paths.add(path.toString());
            return;
        }
        for (int i=0;i<queryMetric.getTags().get(pos2path.get(depth+1)).size();i++)
        {
            pos.set(depth, i);
            dfsInsert(depth + 1, Paths, pos2path, queryMetric, pos);
        }
    }

}
