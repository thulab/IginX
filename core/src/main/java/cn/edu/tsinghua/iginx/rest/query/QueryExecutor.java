package cn.edu.tsinghua.iginx.rest.query;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorNone;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorType;
import cn.edu.tsinghua.iginx.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class QueryExecutor
{
    private final IMetaManager metaManager = SortedListAbstractMetaManager.getInstance();
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutor.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();

    private Query query;

    private RestSession session = new RestSession();


    public QueryExecutor(Query query)
    {
        this.query = query;
    }

    public QueryResult execute(boolean isDelete) throws Exception
    {
        QueryResult ret = new QueryResult();
        try
        {
            session.openSession();
            for (QueryMetric queryMetric : query.getQueryMetrics())
            {
                List<String> paths = getPaths(queryMetric);
                if (isDelete)
                {
                    RestSession session = new RestSession();
                    session.openSession();
                    session.deleteDataInColumns(paths, query.getStartAbsolute(), query.getEndAbsolute());
                    session.closeSession();
                }
                else if (queryMetric.getAggregators().size() == 0)
                {
                    ret.addResultSet(new QueryAggregatorNone().doAggregate(session, paths, query.getStartAbsolute(), query.getEndAbsolute()), queryMetric, new QueryAggregatorNone());
                }
                else
                {
                    for (QueryAggregator queryAggregator : queryMetric.getAggregators())
                    {
                        ret.addResultSet(queryAggregator.doAggregate(session, paths, query.getStartAbsolute(), query.getEndAbsolute()), queryMetric, queryAggregator);
                    }
                }
            }
            session.closeSession();
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during executing", e);
            throw e;
        }

        return ret;
    }

    public List<String> getPaths(QueryMetric queryMetric) throws Exception
    {
        List<String> ret = new ArrayList<>();
        Map<String, Integer> metricschema = metaManager.getSchemaMapping(queryMetric.getName());
        if (metricschema == null)
        {
            throw new Exception("No metadata found");
        }
        else
        {
            Map<Integer, String> pos2path = new TreeMap<>();
            for (Map.Entry<String, Integer> entry: metricschema.entrySet())
                pos2path.put(entry.getValue(), entry.getKey());
            List<Integer> pos = new ArrayList<>();
            for (int i=0;i < pos2path.size();i++)
                pos.add(-1);
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
                String ins = null;
                Map.Entry entry = (Map.Entry) iter.next();
                List<String> tmp = queryMetric.getTags().get(entry.getValue());
                if (tmp != null)
                    ins = queryMetric.getTags().get(entry.getValue()).get(pos.get(now));
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
        if (queryMetric.getTags().get(pos2path.get(depth+1)) == null)
        {
            pos.set(depth, -1);
            dfsInsert(depth + 1, Paths, pos2path, queryMetric, pos);
        }
        else
        {
            for (int i = 0; i < queryMetric.getTags().get(pos2path.get(depth + 1)).size(); i++)
            {
                pos.set(depth, i);
                dfsInsert(depth + 1, Paths, pos2path, queryMetric, pos);
            }
        }
    }
}
