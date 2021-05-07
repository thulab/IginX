package cn.edu.tsinghua.iginx.rest;

public abstract class QueryAggregator
{

    private final QueryAggregatorType type;

    protected QueryAggregator(QueryAggregatorType type) {
        this.type = type;
    }

    public QueryAggregatorType getType() {
        return type;
    }

}
