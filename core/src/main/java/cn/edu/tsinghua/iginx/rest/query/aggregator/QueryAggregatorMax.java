package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.RestSession;
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
                    long maxx = Long.MIN_VALUE;
                    for (int i = 0; i < n; i++)
                    {
                        for (int j = 0; j < m; j++)
                        {
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                maxx = max(maxx, (long) sessionQueryDataSet.getValues().get(i).get(j));
                                datapoints += 1;
                            }

                        }
                        if (i == n - 1 || RestUtils.getInterval(sessionQueryDataSet.getTimestamps()[i], startTimestamp, getDur()) !=
                                RestUtils.getInterval(sessionQueryDataSet.getTimestamps()[i + 1], startTimestamp, getDur()))
                        {
                            queryResultDataset.add(RestUtils.getIntervalStart(sessionQueryDataSet.getTimestamps()[i], startTimestamp, getDur()), maxx);
                            maxx = Long.MIN_VALUE;
                        }
                    }
                    break;
                case DOUBLE:
                    double maxxd = Double.MIN_VALUE;
                    for (int i = 0; i < n; i++)
                    {
                        for (int j = 0; j < m; j++)
                        {
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                maxxd = max(maxxd, (double) sessionQueryDataSet.getValues().get(i).get(j));
                                datapoints += 1;
                            }

                        }
                        if (i == n - 1 || RestUtils.getInterval(sessionQueryDataSet.getTimestamps()[i], startTimestamp, getDur()) !=
                                RestUtils.getInterval(sessionQueryDataSet.getTimestamps()[i + 1], startTimestamp, getDur()))
                        {
                            queryResultDataset.add(RestUtils.getIntervalStart(sessionQueryDataSet.getTimestamps()[i], startTimestamp, getDur()), maxxd);
                            maxxd = Double.MIN_VALUE;
                        }
                    }
                    break;
                default:
                    throw new Exception("Unsupported data type");
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
