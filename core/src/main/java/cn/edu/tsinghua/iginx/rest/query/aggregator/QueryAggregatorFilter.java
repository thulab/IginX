package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.RestUtils;

import java.util.List;

public class QueryAggregatorFilter extends QueryAggregator
{
    public QueryAggregatorFilter()
    {
        super(QueryAggregatorType.FILTER);
    }

    @Override
    public QueryResultDataset doAggregate(RestSession session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try
        {
            SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startTimestamp, endTimestamp);
            DataType type = RestUtils.checkType(sessionQueryDataSet);
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            int datapoints = 0;
            switch (type)
            {
                case LONG:
                    for (int i = 0; i < n; i++)
                    {
                        Long now = null;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                if (now == null && filted((long)sessionQueryDataSet.getValues().get(i).get(j), getFilter()))
                                    now = (long)sessionQueryDataSet.getValues().get(i).get(j);
                                datapoints += 1;
                            }
                        if (now != null)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], now);
                    }
                    queryResultDataset.setSampleSize(datapoints);
                    break;
                case DOUBLE:
                    for (int i = 0; i < n; i++)
                    {
                        Double nowd = null;
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                if (nowd == null && filted((double)sessionQueryDataSet.getValues().get(i).get(j), getFilter()))
                                    nowd = (double)sessionQueryDataSet.getValues().get(i).get(j);
                                datapoints += 1;
                            }
                        if (nowd != null)
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], nowd);
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

    boolean filted(double now, Filter filter)
    {
        switch (filter.getOp())
        {
            case "gt":
                if (now > filter.getValue()) return true;
                else return false;
            case "gte":
                if (now >= filter.getValue()) return true;
                else return false;
            case "eq":
                if (now == filter.getValue()) return true;
                else return false;
            case "ne":
                if (now != filter.getValue()) return true;
                else return false;
            case "lte":
                if (now <= filter.getValue()) return true;
                else return false;
            case "lt":
                if (now < filter.getValue()) return true;
                else return false;
            default:
                return false;
        }
    }
}
