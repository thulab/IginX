package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.RestUtils;

import java.util.List;

public class QueryAggregatorAvg extends QueryAggregator
{

    public QueryAggregatorAvg()
    {
        super(QueryAggregatorType.AVG);
    }


    @Override
    public QueryResultDataset doAggregate(Session session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try
        {
            SessionQueryDataSet sessionQueryDataSet = session.downsampleQuery(paths,
                    startTimestamp, endTimestamp, AggregateType.AVG, getDur());
            SessionQueryDataSet sessionQueryDataSetcnt = session.downsampleQuery(paths,
                    startTimestamp, endTimestamp, AggregateType.COUNT, getDur());
            DataType type = RestUtils.checkType(sessionQueryDataSet);
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            switch (type)
            {
                case LONG:
                case DOUBLE:
                    int datapoints = 0;
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        double sum = 0;
                        long cnt = 0;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                sum += (double) sessionQueryDataSet.getValues().get(i).get(j);
                            }
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSetcnt.getValues().get(i).get(j) != null)
                            {
                                cnt += (long) sessionQueryDataSetcnt.getValues().get(i).get(j);
                            }
                        datapoints += cnt;
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], sum / cnt);
                    }
                    queryResultDataset.setSampleSize(datapoints);
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
