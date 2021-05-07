package cn.edu.tsinghua.iginx.rest;

public enum QueryAggregatorType
{
    MAX("max"),
    MIN("min"),
    SUM("sum"),
    COUNT("count"),
    AVG("avg"),
    FIRST("first"),
    LAST("last");
    private String type;

    QueryAggregatorType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

}
