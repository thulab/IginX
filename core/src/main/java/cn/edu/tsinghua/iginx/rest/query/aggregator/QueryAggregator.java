package cn.edu.tsinghua.iginx.rest.query.aggregator;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.query.QueryResultDataset;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;

import java.util.List;

public abstract class QueryAggregator
{

    private Long Dur;
    public void setDur(Long dur)
    {
        Dur = dur;
    }
    public Long getDur()
    {
        return Dur;
    }

    private QueryAggregatorType type;

    protected QueryAggregator(QueryAggregatorType type)
    {
        this.type = type;
    }

    public QueryAggregatorType getType() {
        return type;
    }

    public AggregateType getAggregateType()
    {
        switch (type)
        {
            case MAX:
                return AggregateType.MAX;
            case MIN:
                return AggregateType.MIN;
            case SUM:
                return AggregateType.SUM;
            case COUNT:
                return AggregateType.COUNT;
            case AVG:
                return AggregateType.AVG;
            case FIRST:
                return AggregateType.FIRST;
            case LAST:
                return AggregateType.LAST;
            default:
                return null;
        }
    }

    public QueryResultDataset doAggregate(Session session, List<String> paths, long startTimestamp, long endTimestamp)
    {
        QueryResultDataset queryResultDataset = new QueryResultDataset();
        try
        {
            SessionQueryDataSet sessionQueryDataSet = session.queryData(paths, startTimestamp, endTimestamp);
            int n = sessionQueryDataSet.getTimestamps().length;
            int m = sessionQueryDataSet.getPaths().size();
            for (int i=0;i<n;i++)
            {
                for (int j=0;j<m;j++)
                    if (sessionQueryDataSet.getValues().get(i).get(j) != null)
                    {
                        queryResultDataset.add(sessionQueryDataSet.getTimestamps()[i], sessionQueryDataSet.getValues().get(i).get(j));
                        break;
                    }
            }
        }
        catch (SessionException e)
        {
            e.printStackTrace();
        }
        return queryResultDataset;
    }
}
