package cn.edu.tsinghua.iginx.rest;

import java.util.ArrayList;
import java.util.List;

public class Query
{
    private Long startAbsolute;
    private Long endAbsolute;
    private Long cacheTime;
    private String timeZone;
    private List<QueryMetric> queryMetrics = new ArrayList<>();

    public List<QueryMetric> getQueryMetrics()
    {
        return queryMetrics;
    }

    public Long getCacheTime()
    {
        return cacheTime;
    }

    public Long getEndAbsolute()
    {
        return endAbsolute;
    }

    public Long getStartAbsolute()
    {
        return startAbsolute;
    }

    public String getTimeZone()
    {
        return timeZone;
    }

    public void setCacheTime(Long cacheTime)
    {
        this.cacheTime = cacheTime;
    }

    public void setEndAbsolute(Long endAbsolute)
    {
        this.endAbsolute = endAbsolute;
    }

    public void setQueryMetrics(List<QueryMetric> queryMetrics)
    {
        this.queryMetrics = queryMetrics;
    }

    public void setStartAbsolute(Long startAbsolute)
    {
        this.startAbsolute = startAbsolute;
    }

    public void setTimeZone(String timeZone)
    {
        this.timeZone = timeZone;
    }

    public void addQueryMetrics(QueryMetric queryMetric)
    {
        this.queryMetrics.add(queryMetric);
    }

}
