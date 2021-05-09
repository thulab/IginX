package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;

import java.util.List;

//todo
public class AggregatorCount extends QueryAggregator
{
    public AggregatorCount() {
        super(QueryAggregatorType.COUNT);
    }

    @Override
    public AggregateType getAggregateType()
    {
        return AggregateType.COUNT;
    }

    @Override
    public QueryResultDataset doAggregate(Session session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try
        {
            SessionQueryDataSet sessionQueryDataSet = session.downsampleQuery(paths,
                    startTimestamp, endTimestamp, getAggregateType(), getDur());
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            for (int i=0;i<n;i++)
            {
                boolean flag = false;
                long cnt = 0;
                for (int j=0;j<m;j++)
                    if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                    {
                        flag = true;
                        cnt += (long)sessionQueryDataSet.getValues().get(i).get(j);
                    }
                if (flag)
                    queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], cnt);
            }
        }
        catch (SessionException e)
        {
            e.printStackTrace();
        }
        return queryResultDataset;
    }
}
