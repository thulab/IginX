package cn.edu.tsinghua.iginx.rest.query.aggregator;

public enum QueryAggregatorType
{
    MAX("max"),
    MIN("min"),
    SUM("sum"),
    COUNT("count"),
    AVG("avg"),
    FIRST("first"),
    LAST("last"),
    DEV("dev"),
    DIFF("diff"),
    DIV("div"),
    NONE("");
    private String type;

    QueryAggregatorType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

}
