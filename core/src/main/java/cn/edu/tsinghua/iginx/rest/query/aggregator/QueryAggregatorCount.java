package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;

import java.util.List;

//todo
public class QueryAggregatorCount extends QueryAggregator
{
    public QueryAggregatorCount() {
        super(QueryAggregatorType.COUNT);
    }


    @Override
    public QueryResultDataset doAggregate(Session session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try
        {
            SessionQueryDataSet sessionQueryDataSet = session.downsampleQuery(paths,
                    startTimestamp, endTimestamp, AggregateType.COUNT, getDur());
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            int datapoints = 0;
            for (int i=0;i<n;i++)
            {
                boolean flag = false;
                long cnt = 0;
                for (int j=0;j<m;j++)
                    if (sessionQueryDataSet.getValues().get(i).get(j) != null && (long)sessionQueryDataSet.getValues().get(i).get(j) != 0)
                    {
                        flag = true;
                        cnt += (long)sessionQueryDataSet.getValues().get(i).get(j);
                    }
                datapoints += cnt;
                if (flag)
                    queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], cnt);
            }
            queryResultDataset.setSampleSize(datapoints);
        }
        catch (SessionException e)
        {
            e.printStackTrace();
        }
        return queryResultDataset;
    }
}
