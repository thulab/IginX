package cn.edu.tsinghua.iginx.rest.query;

import cn.edu.tsinghua.iginx.rest.query.aggregator.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Iterator;

public class QueryParser

{
    private ObjectMapper mapper = new ObjectMapper();
    public QueryParser()
    {

    }

    public Query parseQueryMetric(String json)
    {
        Query ret = new Query();
        try
        {
            JsonNode node = mapper.readTree(json);
            ret = getQuery(node);
        }
        catch (Exception e)
        {

        }
        return ret;
    }

    private Query getQuery(JsonNode node)
    {
        Query ret = new Query();
        JsonNode start_absolute = node.get("start_absolute");
        JsonNode end_absolute = node.get("end_absolute");
        Long now = System.currentTimeMillis();
        if (start_absolute == null && end_absolute == null)
            return null;
        else if (start_absolute != null && end_absolute != null)
        {
            ret.setStartAbsolute(start_absolute.asLong());
            ret.setEndAbsolute(end_absolute.asLong());
        }
        else if (start_absolute != null)
        {
            ret.setStartAbsolute(start_absolute.asLong());
            JsonNode end_relative = node.get("end_relative");
            if (end_relative == null)
                ret.setEndAbsolute(now);
            else
            {
                JsonNode value = end_relative.get("value");
                if (value == null) return null;
                long v = value.asLong();
                JsonNode unit = end_relative.get("unit");
                if (unit == null) return null;
                String u = unit.asText();
                switch (u)
                {
                    case "millis":
                        ret.setEndAbsolute(now - v * 1L);
                        break;
                    case "seconds":
                        ret.setEndAbsolute(now - v * 1000L);
                        break;
                    case "minutes":
                        ret.setEndAbsolute(now - v * 60000L);
                        break;
                    case "hours":
                        ret.setEndAbsolute(now - v * 3600000L);
                        break;
                    case "days":
                        ret.setEndAbsolute(now - v * 86400000L);
                        break;
                    case "weeks":
                        ret.setEndAbsolute(now - v * 604800000L);
                        break;
                    case "months":
                        ret.setEndAbsolute(now - v * 2419200000L);
                        break;
                    case "years":
                        ret.setEndAbsolute(now - v * 29030400000L);
                        break;
                    default:
                        ret.setEndAbsolute(now);
                        break;
                }
            }
        }
        else
        {

            ret.setEndAbsolute(end_absolute.asLong());
            JsonNode start_relative = node.get("start_relative");
            if (start_relative == null)
                ret.setStartAbsolute(now);
            else
            {
                JsonNode value = start_relative.get("value");
                if (value == null) return null;
                long v = value.asLong();
                JsonNode unit = start_relative.get("unit");
                if (unit == null) return null;
                String u = value.asText();
                switch (u)
                {
                    case "millis":
                        ret.setStartAbsolute(now - v * 1L);
                        break;
                    case "seconds":
                        ret.setStartAbsolute(now - v * 1000L);
                        break;
                    case "minutes":
                        ret.setStartAbsolute(now - v * 60000L);
                        break;
                    case "hours":
                        ret.setStartAbsolute(now - v * 3600000L);
                        break;
                    case "days":
                        ret.setStartAbsolute(now - v * 86400000L);
                        break;
                    case "weeks":
                        ret.setStartAbsolute(now - v * 604800000L);
                        break;
                    case "months":
                        ret.setStartAbsolute(now - v * 2419200000L);
                        break;
                    case "years":
                        ret.setStartAbsolute(now - v * 29030400000L);
                        break;
                    default:
                        ret.setStartAbsolute(now);
                        break;
                }
            }
        }
        JsonNode cacheTime = node.get("cacheTime");
        if (cacheTime != null)
            ret.setCacheTime(cacheTime.asLong());
        JsonNode timeZone = node.get("time_zone");
        if (cacheTime != null)
            ret.setTimeZone(timeZone.asText());

        JsonNode metrics = node.get("metrics");
        if (metrics != null && metrics.isArray())
        {
            for (JsonNode dpnode : metrics)
            {
                QueryMetric ins = new QueryMetric();
                JsonNode name = dpnode.get("name");
                if (name != null)
                    ins.setName(name.asText());
                JsonNode tags = dpnode.get("tags");
                if (tags != null)
                {
                    Iterator<String> fieldNames = tags.fieldNames();
                    Iterator<JsonNode> elements = tags.elements();
                    while (elements.hasNext() && fieldNames.hasNext())
                    {
                        String key = fieldNames.next();
                        for (JsonNode valuenode : elements.next())
                            ins.addTag(key, valuenode.asText());
                    }
                }
                addAggregators(ins, dpnode);
                ret.addQueryMetrics(ins);
            }
        }
        return ret;
    }

    public void addAggregators(QueryMetric q, JsonNode node)
    {
        JsonNode aggregators = node.get("aggregators");
        if (aggregators == null || !aggregators.isArray())
            return;
        for (JsonNode aggregator: aggregators)
        {
            JsonNode name = aggregator.get("name");
            if (name == null) continue;
            QueryAggregator qa;
            switch (name.asText())
            {
                case "max":
                    qa = new QueryAggregatorMax();
                    break;
                case "min":
                    qa = new QueryAggregatorMin();
                    break;
                case "sum":
                    qa = new QueryAggregatorSum();
                    break;
                case "count":
                    qa = new QueryAggregatorCount();
                    break;
                case "avg":
                    qa = new QueryAggregatorAvg();
                    break;
                case "first":
                    qa = new QueryAggregatorFirst();
                    break;
                case "last":
                    qa = new QueryAggregatorLast();
                    break;
                case "dev":
                    qa = new QueryAggregatorDev();
                    break;
                default:
                    continue;
            }
            switch (name.asText())
            {
                case "max":
                case "min":
                case "sum":
                case "count":
                case "avg":
                case "first":
                case "last":
                case "dev":
                    JsonNode sampling = aggregator.get("sampling");
                    if (sampling == null) break;
                    JsonNode value = sampling.get("value");
                    if (value == null) break;
                    JsonNode unit = sampling.get("unit");
                    if (unit == null) break;
                    switch (unit.asText())
                    {
                        case "millis":
                            qa.setDur(value.asLong() * 1L);
                            break;
                        case "seconds":
                            qa.setDur(value.asLong() * 1000L);
                            break;
                        case "minutes":
                            qa.setDur(value.asLong() * 60000L);
                            break;
                        case "hours":
                            qa.setDur(value.asLong() * 3600000L);
                            break;
                        case "days":
                            qa.setDur(value.asLong() * 86400000L);
                            break;
                        case "weeks":
                            qa.setDur(value.asLong() * 604800000L);
                            break;
                        case "months":
                            qa.setDur(value.asLong() * 2419200000L);
                            break;
                        case "years":
                            qa.setDur(value.asLong() * 29030400000L);
                            break;
                        default:
                            continue;
                }
            }
            q.addAggregator(qa);
        }
    }

    public String parseResultToJson(QueryResult result)
    {
        StringBuilder ret = new StringBuilder("{\"queries\":[");
        for (int i=0; i< result.siz; i++)
        {
            ret.append(result.toResultString(i));
        }
        ret.append("]}");
        return ret.toString();
    }
}
