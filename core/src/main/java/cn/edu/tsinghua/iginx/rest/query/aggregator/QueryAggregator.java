package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;

import java.util.List;

public abstract class QueryAggregator
{
    private Double divisor;

    public void setDivisor(Double divisor)
    {
        this.divisor = divisor;
    }

    public Double getDivisor()
    {
        return divisor;
    }

    private Long Dur;
    public void setDur(Long dur)
    {
        Dur = dur;
    }
    public Long getDur()
    {
        return Dur;
    }

    private double Percentile;


    public void setPercentile(double percentile)
    {
        Percentile = percentile;
    }

    public double getPercentile()
    {
        return Percentile;
    }

    private long Unit;

    public long getUnit()
    {
        return Unit;
    }

    public void setUnit(long unit)
    {
        Unit = unit;
    }

    private String metric_name;

    public void setMetric_name(String metric_name)
    {
        this.metric_name = metric_name;
    }

    public String getMetric_name()
    {
        return metric_name;
    }

    private Filter filter;

    public void setFilter(Filter filter)
    {
        this.filter = filter;
    }

    public Filter getFilter()
    {
        return filter;
    }

    public void setType(QueryAggregatorType type)
    {
        this.type = type;
    }

    private QueryAggregatorType type;

    protected QueryAggregator(QueryAggregatorType type)
    {
        this.type = type;
    }

    public QueryAggregatorType getType() {
        return type;
    }


    public QueryResultDataset doAggregate(RestSession session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startTimestamp, endTimestamp);
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
                    }
                    datapoints += 1;
                }
        }
        queryResultDataset.setSampleSize(datapoints);
        return queryResultDataset;
    }
}
