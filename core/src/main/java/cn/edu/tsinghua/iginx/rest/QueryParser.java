package cn.edu.tsinghua.iginx.rest;

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
            if (end_relative == null) return null;
            JsonNode value = end_relative.get("value");
            if (value == null) return null;
            long v = value.asLong();
            JsonNode unit = end_relative.get("unit");
            if (unit == null) return null;
            String u = unit.asText();
            switch (u)
            {
                case "millis":
                    ret.setEndAbsolute(start_absolute.asLong() + v * 1L);
                    break;
                case "seconds":
                    ret.setEndAbsolute(start_absolute.asLong() + v * 1000L);
                    break;
                case "minutes":
                    ret.setEndAbsolute(start_absolute.asLong() + v * 60000L);
                    break;
                case "hours":
                    ret.setEndAbsolute(start_absolute.asLong() + v * 3600000L);
                    break;
                case "days":
                    ret.setEndAbsolute(start_absolute.asLong() + v * 86400000L);
                    break;
                case "weeks":
                    ret.setEndAbsolute(start_absolute.asLong() + v * 604800000L);
                    break;
                case "months":
                    ret.setEndAbsolute(start_absolute.asLong() + v * 2419200000L);
                    break;
                case "years":
                    ret.setEndAbsolute(start_absolute.asLong() + v * 29030400000L);
                    break;
                default:
                    ret.setEndAbsolute(start_absolute.asLong());
                    break;
            }
        }
        else
        {

            ret.setEndAbsolute(end_absolute.asLong());
            JsonNode start_relative = node.get("start_relative");
            if (start_relative == null) return null;
            JsonNode value = start_relative.get("value");
            if (value == null) return null;
            long v = value.asLong();
            JsonNode unit = start_relative.get("unit");
            if (unit == null) return null;
            String u = value.asText();
            switch (u)
            {
                case "millis":
                    ret.setStartAbsolute(end_absolute.asLong() - v * 1L);
                    break;
                case "seconds":
                    ret.setStartAbsolute(end_absolute.asLong() - v * 1000L);
                    break;
                case "minutes":
                    ret.setStartAbsolute(end_absolute.asLong() - v * 60000L);
                    break;
                case "hours":
                    ret.setStartAbsolute(end_absolute.asLong() - v * 3600000L);
                    break;
                case "days":
                    ret.setStartAbsolute(end_absolute.asLong() - v * 86400000L);
                    break;
                case "weeks":
                    ret.setStartAbsolute(end_absolute.asLong() - v * 604800000L);
                    break;
                case "months":
                    ret.setStartAbsolute(end_absolute.asLong() - v * 2419200000L);
                    break;
                case "years":
                    ret.setStartAbsolute(end_absolute.asLong() - v * 29030400000L);
                    break;
                default:
                    ret.setStartAbsolute(end_absolute.asLong());
                    break;
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
