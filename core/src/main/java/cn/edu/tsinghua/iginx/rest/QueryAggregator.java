package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.thrift.AggregateType;

public class QueryAggregator
{

    private final QueryAggregatorType type;
    private Long Dur;

    public QueryAggregator(String type) {
        switch (type)
        {
            case "max":
                this.type = QueryAggregatorType.MAX;
                break;
            case "min":
                this.type = QueryAggregatorType.MIN;
                break;
            case "sum":
                this.type = QueryAggregatorType.SUM;
                break;
            case "count":
                this.type = QueryAggregatorType.COUNT;
                break;
            case "avg":
                this.type = QueryAggregatorType.AVG;
                break;
            case "first":
                this.type = QueryAggregatorType.FIRST;
                break;
            case "last":
                this.type = QueryAggregatorType.LAST;
                break;
            default:
                this.type = QueryAggregatorType.MAX;
                break;
        }
    }

    public QueryAggregatorType getType() {
        return type;
    }

    public void setDur(Long dur)
    {
        Dur = dur;
    }

    public Long getDur()
    {
        return Dur;
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
}
