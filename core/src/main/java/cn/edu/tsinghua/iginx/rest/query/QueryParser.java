/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.rest.query;

import cn.edu.tsinghua.iginx.rest.query.aggregator.Filter;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorAvg;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorCount;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorDev;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorDiff;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorDiv;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorFilter;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorFirst;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorLast;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorMax;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorMin;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorPercentile;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorRate;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorSampler;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorSaveAs;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorSum;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

public class QueryParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);
    private ObjectMapper mapper = new ObjectMapper();

    public QueryParser() {

    }

    public Query parseGrafanaQueryMetric(String json) throws Exception
    {
        Query ret;
        try
        {
            JsonNode node = mapper.readTree(json);
            ret = getGrafanaQuery(node);
        }
        catch (Exception e)
        {
            LOGGER.error("Error occurred during parsing query ", e);
            throw e;
        }
        return ret;
    }

    public Query parseQueryMetric(String json) throws Exception {
        Query ret;
        try {
            JsonNode node = mapper.readTree(json);
            ret = getQuery(node);
        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing query ", e);
            throw e;
        }
        return ret;
    }

    public Query parseAnnotationQueryMetric(String json) throws Exception {
        Query ret;
        try {
            JsonNode node = mapper.readTree(json);
            ret = getAnnotationQuery(node);
        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing query ", e);
            throw e;
        }
        return ret;
    }
    public static Long dealDateFormat(String oldDateStr)
    {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        try
        {
            Date date = df.parse(oldDateStr);
            return date.getTime() + 28800000l;
        }
        catch (ParseException e)
        {
            e.printStackTrace();
        }
        return null;
    }


    private Query getGrafanaQuery(JsonNode node)
    {
        Query ret = new Query();
        JsonNode timerange = node.get("range");
        if (timerange == null)
            return null;

        JsonNode start_absolute = timerange.get("from");
        JsonNode end_absolute = timerange.get("to");


        if (start_absolute == null || end_absolute == null)
            return null;

        Long start = dealDateFormat(start_absolute.asText());
        Long end = dealDateFormat(end_absolute.asText());
        ret.setStartAbsolute(start);
        ret.setEndAbsolute(end);

        JsonNode array = node.get("targets");
        if (!array.isArray())
             return null;
        for (JsonNode jsonNode: array)
        {
            QueryMetric queryMetric = new QueryMetric();
            JsonNode type = jsonNode.get("type");
            if (type == null)
                return null;
            JsonNode target = jsonNode.get("target");
            if (target == null)
                return null;
            if (type.asText().equals("table"))
            {
                queryMetric.setName(target.asText());
            }
            else
            {
                queryMetric.setName(target.asText());
            }
            ret.addQueryMetrics(queryMetric);
        }
        return ret;
    }

    private Query getQuery(JsonNode node) {
        Query ret = new Query();
        JsonNode start_absolute = node.get("start_absolute");
        JsonNode end_absolute = node.get("end_absolute");
        Long now = System.currentTimeMillis();
        if (start_absolute == null && end_absolute == null)
            return null;
        else if (start_absolute != null && end_absolute != null) {
            ret.setStartAbsolute(start_absolute.asLong());
            ret.setEndAbsolute(end_absolute.asLong());
        } else if (start_absolute != null) {
            ret.setStartAbsolute(start_absolute.asLong());
            JsonNode end_relative = node.get("end_relative");
            if (end_relative == null)
                ret.setEndAbsolute(now);
            else {
                JsonNode value = end_relative.get("value");
                if (value == null) return null;
                long v = value.asLong();
                JsonNode unit = end_relative.get("unit");
                if (unit == null) return null;
                String u = unit.asText();
                switch (u) {
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
        } else {

            ret.setEndAbsolute(end_absolute.asLong());
            JsonNode start_relative = node.get("start_relative");
            if (start_relative == null)
                ret.setStartAbsolute(now);
            else {
                JsonNode value = start_relative.get("value");
                if (value == null) return null;
                long v = value.asLong();
                JsonNode unit = start_relative.get("unit");
                if (unit == null) return null;
                String u = value.asText();
                switch (u) {
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
        if (metrics != null && metrics.isArray()) {
            for (JsonNode dpnode : metrics) {
                QueryMetric ins = new QueryMetric();
                JsonNode name = dpnode.get("name");
                if (name != null)
                    ins.setName(name.asText());
                JsonNode tags = dpnode.get("tags");
                if (tags != null) {
                    Iterator<String> fieldNames = tags.fieldNames();
                    Iterator<JsonNode> elements = tags.elements();
                    while (elements.hasNext() && fieldNames.hasNext()) {
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

    private Query getAnnotationQuery(JsonNode node) throws JsonProcessingException
    {
        Query ret = new Query();
        JsonNode range = node.get("range");
        if (range == null)
            return null;
        JsonNode start_absolute = range.get("from");
        JsonNode end_absolute = range.get("to");
        if (start_absolute == null || end_absolute == null)
            return null;
        else
        {
            Long start = dealDateFormat(start_absolute.asText());
            Long end = dealDateFormat(end_absolute.asText());
            ret.setStartAbsolute(start);
            ret.setEndAbsolute(end);
        }

        JsonNode metric = node.get("annotation");
        if (metric == null)
            return null;
        QueryMetric ins = new QueryMetric();
        JsonNode name = metric.get("name");
        if (name != null)
            ins.setName(name.asText());
        JsonNode tags = metric.get("query");
        if (tags != null)
        {
            if (tags.get("tags") == null)
                tags = mapper.readTree(tags.asText());
            tags = tags.get("tags");
            Iterator<String> fieldNames = tags.fieldNames();
            while (fieldNames.hasNext())
            {
                String key = fieldNames.next();
                JsonNode valuenode = tags.get(key);
                ins.addTag(key, valuenode.asText());
            }
        }
        ins.setAnnotation(true);
        ret.addQueryMetrics(ins);

        return ret;
    }


    public void addAggregators(QueryMetric q, JsonNode node) {
        JsonNode aggregators = node.get("aggregators");
        if (aggregators == null || !aggregators.isArray())
            return;
        for (JsonNode aggregator : aggregators) {
            JsonNode name = aggregator.get("name");
            if (name == null) continue;
            QueryAggregator qa;
            switch (name.asText()) {
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
            switch (name.asText()) {
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
                    switch (unit.asText()) {
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
                    switch (unit.asText()) {
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
                    switch (unit.asText()) {
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
            switch (name.asText()) {
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

    public String parseResultToJson(QueryResult result, boolean isDelete) {
        if (isDelete)
            return "";
        StringBuilder ret = new StringBuilder("{\"queries\":[");
        for (int i = 0; i < result.getSiz(); i++) {
            ret.append(result.toResultString(i));
            ret.append(",");
        }
        if (ret.charAt(ret.length() - 1) == ',')
            ret.deleteCharAt(ret.length() - 1);
        ret.append("]}");
        return ret.toString();
    }

    public String parseResultToGrafanaAnnotationJson(QueryResult result)
    {
        StringBuilder ret = new StringBuilder("[");
        ret.append(result.toAnnotationResultString());
        ret.append("]");
        return ret.toString();
    }

    public String parseResultToGrafanaJson(QueryResult result)
    {
        StringBuilder ret = new StringBuilder("[");
        for (int i=0; i< result.getSiz(); i++)
        {
            ret.append("{");
            ret.append(String.format("\"target\":\"%s\",", result.getQueryMetrics().get(i).getName()));
            ret.append("\"datapoints\":[");
            int n = result.getQueryResultDatasets().get(i).getSize();
            for (int j=0;j<n;j++)
            {
                ret.append("[");
                if (result.getQueryResultDatasets().get(i).getValues().get(j) instanceof byte[])
                    ret.append(result.getQueryResultDatasets().get(i).getValues().get(j));
                else
                    ret.append(result.getQueryResultDatasets().get(i).getValues().get(j).toString());

                ret.append(String.format(",%d", result.getQueryResultDatasets().get(i).getTimestamps().get(j)));
                ret.append("],");
            }
            if (ret.charAt(ret.length()-1) == ',')
                ret.deleteCharAt(ret.length()-1);
            ret.append("]},");
        }
        if (ret.charAt(ret.length()-1) == ',')
            ret.deleteCharAt(ret.length()-1);
        ret.append("]");
        return ret.toString();
    }
}
