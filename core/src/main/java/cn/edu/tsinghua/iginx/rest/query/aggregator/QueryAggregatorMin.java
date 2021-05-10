package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.RestUtils;

import java.util.List;

import static java.lang.Math.min;

public class QueryAggregatorMin extends QueryAggregator
{
    public QueryAggregatorMin() {
        super(QueryAggregatorType.MIN);
    }


    @Override
    public QueryResultDataset doAggregate(Session session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try
        {
            SessionQueryDataSet sessionQueryDataSet = session.downsampleQuery(paths,
                    startTimestamp, endTimestamp, AggregateType.MIN, getDur());
            SessionQueryDataSet sessionQueryDataSetcnt = session.downsampleQuery(paths,
                    startTimestamp, endTimestamp, AggregateType.COUNT, getDur());
            DataType type = RestUtils.checkType(sessionQueryDataSet);
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            int datapoints = 0;
            switch (type)
            {
                case BOOLEAN:
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        boolean minn = true;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                minn &= (boolean) sessionQueryDataSet.getValues().get(i).get(j);
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], minn);
                    }
                    break;
                case LONG:
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        long minn = Long.MAX_VALUE;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                minn = min(minn, (long) sessionQueryDataSet.getValues().get(i).get(j));
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], minn);
                    }
                    break;
                case DOUBLE:
                    for (int i = 0; i < n; i++)
                    {
                        boolean flag = false;
                        double minn = Double.MAX_VALUE;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                flag = true;
                                minn = min(minn, (double) sessionQueryDataSet.getValues().get(i).get(j));
                            }
                        if (flag)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], minn);
                    }
                    break;
                default:
                    throw new Exception("Unsupported data type");
            }
            for (int i=0;i<n;i++)
            {
                long cnt = 0;
                for (int j=0;j<m;j++)
                    if (sessionQueryDataSetcnt.getValues().get(i).get(j) != null && (long)sessionQueryDataSetcnt.getValues().get(i).get(j) != 0)
                    {
                        cnt += (long)sessionQueryDataSetcnt.getValues().get(i).get(j);
                    }
                datapoints += cnt;
            }
            queryResultDataset.setSampleSize(datapoints);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return queryResultDataset;
    }
}
