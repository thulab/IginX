package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.RestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryAggregatorRate extends QueryAggregator
{
    public QueryAggregatorRate()
    {
        super(QueryAggregatorType.RATE);
    }

    @Override
    public QueryResultDataset doAggregate(RestSession session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try
        {
            SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startTimestamp, endTimestamp);
            queryResultDataset.setPaths(sessionQueryDataSet.getPaths());
            DataType type = RestUtils.checkType(sessionQueryDataSet);
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            int datapoints = 0;
            switch (type)
            {

                case LONG:
                case DOUBLE:
                    Double lastd = null;
                    Double nowd = null;
                    for (int i = 0; i < n; i++)
                    {
                        for (int j = 0; j < m; j++)
                            if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                            {
                                if (nowd == null)
                                    nowd = (double)sessionQueryDataSet.getValues().get(i).get(j);
                                datapoints += 1;
                            }
                        if (i != 0)
                        {
                            queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], (nowd - lastd) * getUnit() /
                                    (sessionQueryDataSet.getTimestamps()[i] - sessionQueryDataSet.getTimestamps()[i - 1]));
                        }
                        lastd = nowd;
                        nowd = null;
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
