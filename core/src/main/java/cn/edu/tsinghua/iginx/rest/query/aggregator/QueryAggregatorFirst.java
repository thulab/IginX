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

//todo
public class QueryAggregatorFirst extends QueryAggregator
{
    public QueryAggregatorFirst() {
        super(QueryAggregatorType.FIRST);
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
            switch (type)
            {
                case BOOLEAN:
                case LONG:
                case DOUBLE:
                case BINARY:
                    Object ins = null;
                    int datapoints = 0;
                    for (int i = 0; i < n; i++)
                    {
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                if (ins == null) ins = sessionQueryDataSet.getValues().get(i).get(j);
                                datapoints += 1;
                            }
                        if (i == n - 1 || RestUtils.getInterval(sessionQueryDataSet.getTimestamps()[i], startTimestamp, getDur()) !=
                                RestUtils.getInterval(sessionQueryDataSet.getTimestamps()[i + 1], startTimestamp, getDur()))
                        {
                            queryResultDataset.add(RestUtils.getIntervalStart(sessionQueryDataSet.getTimestamps()[i], startTimestamp, getDur()), ins);
                            ins = null;
                        }
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
