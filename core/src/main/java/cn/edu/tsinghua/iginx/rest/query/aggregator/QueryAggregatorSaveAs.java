package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.insert.DataPointsParser;
import cn.edu.tsinghua.iginx.rest.insert.Metric;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class QueryAggregatorSaveAs extends QueryAggregator
{
    public QueryAggregatorSaveAs() {
        super(QueryAggregatorType.SAVE_AS);
    }

    @Override
    public QueryResultDataset doAggregate(RestSession session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        DataPointsParser parser = new DataPointsParser();
        List<Metric> metrics = new ArrayList<>();
        Metric ins = new Metric();
        String name = paths.get(0).split("\\.")[paths.get(0).split("\\.").length - 1];
        ins.setName(getMetric_name());
        ins.addTag("saved_from", name);
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startTimestamp, endTimestamp);
        queryResultDataset.setPaths(getPathsFromSessionQueryDataSet(sessionQueryDataSet));
        int n = sessionQueryDataSet.getTimestamps().length;
        int m = sessionQueryDataSet.getPaths().size();
        int datapoints = 0;
        for (int i=0;i<n;i++)
        {
            boolean flag = false;
            for (int j=0;j<m;j++)
                if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                {
                    if (!flag)
                    {
                        queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], sessionQueryDataSet.getValues().get(i).get(j));
                        flag = true;
                        ins.addTimestamp(sessionQueryDataSet.getTimestamps()[i]);
                        ins.addValue(sessionQueryDataSet.getValues().get(i).get(j).toString());
                    }
                    datapoints += 1;
                }
        }
        queryResultDataset.setSampleSize(datapoints);
        metrics.add(ins);
        parser.setMetricList(metrics);
        parser.sendData();

        return queryResultDataset;
    }
}
