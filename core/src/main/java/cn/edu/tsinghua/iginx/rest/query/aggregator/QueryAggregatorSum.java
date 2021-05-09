package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.RestUtils;

import java.util.List;

public class QueryAggregatorSum extends QueryAggregator
{
    public QueryAggregatorSum()
    {
        super(QueryAggregatorType.SUM);
    }

    @Override
    public AggregateType getAggregateType()
    {
        return AggregateType.SUM;
    }

    @Override
    public QueryResultDataset doAggregate(Session session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try
        {
            SessionQueryDataSet sessionQueryDataSet = session.downsampleQuery(paths,
                    startTimestamp, endTimestamp, getAggregateType(), getDur());
            DataType type = RestUtils.checkType(sessionQueryDataSet);
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            switch (type)
            {
                case BOOLEAN:
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        boolean sum = false;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                sum |= (boolean) sessionQueryDataSet.getValues().get(i).get(j);
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], sum);
                    }
                    break;
                case LONG:
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        long sum = 0;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                sum += (long) sessionQueryDataSet.getValues().get(i).get(j);
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], sum);
                    }
                    break;
                case DOUBLE:
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        double sum = 0;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                sum += (double) sessionQueryDataSet.getValues().get(i).get(j);
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], sum);
                    }
                    break;
                default:
                    throw new Exception("Unsupported data type");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return queryResultDataset;
    }
}
