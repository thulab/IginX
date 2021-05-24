package cn.edu.tsinghua.iginx.rest.query;

import cn.edu.tsinghua.iginx.rest.insert.DataPointsParser;
import cn.edu.tsinghua.iginx.rest.query.aggregator.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class QueryParser

{
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);
    private ObjectMapper mapper = new ObjectMapper();
    public QueryParser()
    {

    }

    public Query parseQueryMetric(String json) throws Exception
    {
        Query ret;
        try
        {
            JsonNode node = mapper.readTree(json);
            ret = getQuery(node);
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during parsing query ", e);
            throw e;
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
                case "diff":
                    qa = new QueryAggregatorDiff();
                    break;
                case "div":
                    qa = new QueryAggregatorDiv();
                    break;
                case "filter":
                    qa = new QueryAggregatorFilter();
                    break;
                case "save_as":
                    qa = new QueryAggregatorSaveAs();
                    break;
                case "rate":
                    qa = new QueryAggregatorRate();
                    break;
                case "sampler":
                    qa = new QueryAggregatorSampler();
                    break;
                case "percentile":
                    qa = new QueryAggregatorPercentile();
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
                case "percentile":
                    JsonNode sampling = aggregator.get("sampling");
                    if (sampling == null) continue;
                    JsonNode value = sampling.get("value");
                    if (value == null) continue;
                    JsonNode unit = sampling.get("unit");
                    if (unit == null) continue;
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
                    break; 
                case "diff":
                    break;
                case "div":
                    JsonNode divisor = aggregator.get("divisor");
                    if (divisor == null) continue;
                    qa.setDivisor(Double.parseDouble(divisor.asText()));
                    break;
                case "filter":
                    JsonNode filter_op = aggregator.get("filter_op");
                    if (filter_op == null) continue;
                    JsonNode threshold = aggregator.get("threshold");
                    if (threshold == null) continue;
                    qa.setFilter(new Filter(filter_op.asText(), threshold.asDouble()));
                    break;
                case "save_as":
                    JsonNode metric_name = aggregator.get("metric_name");
                    if (metric_name == null) continue;
                    qa.setMetric_name(metric_name.asText());
                    break;
                case "rate":
                    sampling = aggregator.get("sampling");
                    if (sampling == null) continue;
                    unit = sampling.get("unit");
                    if (unit == null) continue;
                    switch (unit.asText())
                    {
                        case "millis":
                            qa.setUnit(1L);
                            break;
                        case "seconds":
                            qa.setUnit(1000L);
                            break;
                        case "minutes":
                            qa.setUnit(60000L);
                            break;
                        case "hours":
                            qa.setUnit(3600000L);
                            break;
                        case "days":
                            qa.setUnit(86400000L);
                            break;
                        case "weeks":
                            qa.setUnit(604800000L);
                            break;
                        case "months":
                            qa.setUnit(2419200000L);
                            break;
                        case "years":
                            qa.setUnit(29030400000L);
                            break;
                        default:
                            continue;
                    }
                    break;
                case "sampler":
                    unit = aggregator.get("unit");
                    if (unit == null) continue;
                    switch (unit.asText())
                    {
                        case "millis":
                            qa.setUnit(1L);
                            break;
                        case "seconds":
                            qa.setUnit(1000L);
                            break;
                        case "minutes":
                            qa.setUnit(60000L);
                            break;
                        case "hours":
                            qa.setUnit(3600000L);
                            break;
                        case "days":
                            qa.setUnit(86400000L);
                            break;
                        case "weeks":
                            qa.setUnit(604800000L);
                            break;
                        case "months":
                            qa.setUnit(2419200000L);
                            break;
                        case "years":
                            qa.setUnit(29030400000L);
                            break;
                        default:
                            continue;
                    }
                    break;
                default:
                    break;
                    
            }
            switch (name.asText())
            {
                case "percentile":
                    JsonNode percentile = aggregator.get("percentile");
                    if (percentile == null) continue;
                    qa.setPercentile(Double.parseDouble(percentile.asText()));
                    break;
                default:
                    break;

            }
            q.addAggregator(qa);
        }
    }

    public String parseResultToJson(QueryResult result, boolean isDelete)
    {
        if (isDelete)
            return "";
        StringBuilder ret = new StringBuilder("{\"queries\":[");
        for (int i=0; i< result.getSiz(); i++)
        {
            ret.append(result.toResultString(i));
            ret.append(",");
        }
        if (ret.charAt(ret.length()-1) == ',')
            ret.deleteCharAt(ret.length()-1);
        ret.append("]}");
        return ret.toString();
    }
}
