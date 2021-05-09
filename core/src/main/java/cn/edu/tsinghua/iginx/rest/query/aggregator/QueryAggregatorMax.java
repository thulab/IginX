package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.RestUtils;

import java.util.List;

import static java.lang.Math.max;

public class QueryAggregatorMax extends QueryAggregator
{
    public QueryAggregatorMax() {
        super(QueryAggregatorType.MAX);
    }

    @Override
    public AggregateType getAggregateType()
    {
        return AggregateType.MAX;
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
                        boolean maxx = false;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                maxx |= (boolean) sessionQueryDataSet.getValues().get(i).get(j);
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], maxx);
                    }
                    break;
                case LONG:
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        long maxx = Long.MIN_VALUE;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                maxx = max(maxx, (long) sessionQueryDataSet.getValues().get(i).get(j));
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], maxx);
                    }
                    break;
                case DOUBLE:
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        double maxx = Double.MIN_VALUE;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                maxx = max(maxx, (double) sessionQueryDataSet.getValues().get(i).get(j));
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], maxx);
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
