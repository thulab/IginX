package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;

import java.util.List;

public class AggregatorCount extends QueryAggregator
{
    public AggregatorCount() {
        super(QueryAggregatorType.COUNT);
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
                for (int j=0;j<m;j++)
                    if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                    {
                        queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], sessionQueryDataSet.getValues().get(i).get(j));
                        break;
                    }
            }
        }
        catch (SessionException e)
        {
            e.printStackTrace();
        }
        return queryResultDataset;
    }
}
