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

import cn.edu.tsinghua.iginx.rest.bean.AnnotationLimit;
import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryMetric;
import cn.edu.tsinghua.iginx.rest.bean.QueryResult;
import cn.edu.tsinghua.iginx.rest.query.aggregator.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

public class QueryParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryParser.class);
    private final ObjectMapper mapper = new ObjectMapper();

    public QueryParser() {

    }

    public static Long dealDateFormat(String oldDateStr) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        try {
            Date date = df.parse(oldDateStr);
            return date.getTime() + 28800000L;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Long transTimeFromString(String str) {
        switch (str) {
            case "millis":
                return 1L;
            case "seconds":
                return 1000L;
            case "minutes":
                return 60000L;
            case "hours":
                return 3600000L;
            case "days":
                return 86400000L;
            case "weeks":
                return 604800000L;
            case "months":
                return 2419200000L;
            case "years":
                return 29030400000L;
            default:
                return 0L;
        }
    }

    public Query parseGrafanaQueryMetric(String json) throws Exception {
        Query ret;
        try {
            JsonNode node = mapper.readTree(json);
            ret = getGrafanaQuery(node);
        } catch (Exception e) {
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

    public Query parseAnnotationQueryMetric(String json, boolean isGrafana) throws Exception {
        Query ret;
        try {
            JsonNode node = mapper.readTree(json);
            ret = getAnnotationQuery(node, isGrafana);
        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing query ", e);
            throw e;
        }
        return ret;
    }

    private Query getGrafanaQuery(JsonNode node) {
        Query ret = new Query();
        JsonNode timerange = node.get("range");
        if (timerange == null) {
            return null;
        }
        JsonNode startAbsolute = timerange.get("from");
        JsonNode end_absolute = timerange.get("to");

        if (startAbsolute == null || end_absolute == null) {
            return null;
        }

        Long start = dealDateFormat(startAbsolute.asText());
        Long end = dealDateFormat(end_absolute.asText());
        ret.setStartAbsolute(start);
        ret.setEndAbsolute(end);

        JsonNode array = node.get("targets");
        if (!array.isArray()) {
            return null;
        }
        for (JsonNode jsonNode : array) {
            QueryMetric queryMetric = new QueryMetric();
            JsonNode type = jsonNode.get("type");
            if (type == null) {
                return null;
            }
            JsonNode target = jsonNode.get("target");
            if (target == null) {
                return null;
            }
            queryMetric.setName(target.asText());
            ret.addQueryMetrics(queryMetric);
        }
        return ret;
    }

    private Query getQuery(JsonNode node) {
        Query ret = new Query();
        JsonNode start_absolute = node.get("start_absolute");
        JsonNode end_absolute = node.get("end_absolute");
        long now = System.currentTimeMillis();
        if (start_absolute == null && end_absolute == null) {
            return null;
        }
        else if (start_absolute != null && end_absolute != null) {
            ret.setStartAbsolute(start_absolute.asLong());
            ret.setEndAbsolute(end_absolute.asLong());
        } else if (start_absolute != null) {
            if (setEndAbsolute(node, ret, start_absolute, now)) {
                return null;
            }
        } else {
            ret.setEndAbsolute(end_absolute.asLong());
            JsonNode start_relative = node.get("start_relative");
            if (start_relative == null) {
                ret.setStartAbsolute(now);
            }
            else {
                JsonNode value = start_relative.get("value");
                if (value == null) {
                    return null;
                }
                long v = value.asLong();
                JsonNode unit = start_relative.get("unit");
                if (unit == null) {
                    return null;
                }
                Long time = transTimeFromString(unit.asText());
                ret.setStartAbsolute(now - v * time);
            }
        }
        JsonNode cacheTime = node.get("cacheTime");
        if (cacheTime != null) {
            ret.setCacheTime(cacheTime.asLong());
        }
        JsonNode timeZone = node.get("time_zone");
        if (cacheTime != null) {
            ret.setTimeZone(timeZone.asText());
        }

        JsonNode metrics = node.get("metrics");
        if (metrics != null && metrics.isArray()) {
            for (JsonNode dpnode : metrics) {
                QueryMetric ins = setQueryMetric(dpnode);
                addAggregators(ins, dpnode);
                ret.addQueryMetrics(ins);
            }
        }
        return ret;
    }

    private QueryMetric setQueryMetric(JsonNode dpnode) {
        QueryMetric ret = new QueryMetric();
        JsonNode name = dpnode.get("name");
        if (name != null) {
            ret.setName(name.asText());
        }
        JsonNode tags = dpnode.get("tags");
        if (tags != null) {
            Iterator<String> fieldNames = tags.fieldNames();
            Iterator<JsonNode> elements = tags.elements();
            while (elements.hasNext() && fieldNames.hasNext()) {
                String key = fieldNames.next();
                for (JsonNode valuenode : elements.next()) {
                    ret.addTag(key, valuenode.asText());
                }
            }
        }
        return ret;
    }

    private boolean setEndAbsolute(JsonNode node, Query ret, JsonNode start_absolute, long now) {
        ret.setStartAbsolute(start_absolute.asLong());
        JsonNode end_relative = node.get("end_relative");
        if (end_relative == null) {
            ret.setEndAbsolute(now);
        }
        else {
            JsonNode value = end_relative.get("value");
            if (value == null) {
                return true;
            }
            long v = value.asLong();
            JsonNode unit = end_relative.get("unit");
            if (unit == null) {
                return true;
            }
            Long time = transTimeFromString(unit.asText());
            ret.setEndAbsolute(now - v * time);
        }
        return false;
    }

    private Query getAnnotationQuery(JsonNode node, boolean isGrafana) throws JsonProcessingException {
        Query ret = new Query();
        if (isGrafana) {
            JsonNode range = node.get("range");
            if (range == null) {
                return null;
            }
            JsonNode start_absolute = range.get("from");
            JsonNode end_absolute = range.get("to");
            if (start_absolute == null || end_absolute == null) {
                return null;
            } else {
                Long start = dealDateFormat(start_absolute.asText());
                Long end = dealDateFormat(end_absolute.asText());
                ret.setStartAbsolute(start);
                ret.setEndAbsolute(end);
            }

            JsonNode metric = node.get("annotation");
            if (metric == null) {
                return null;
            }
            QueryMetric ins = new QueryMetric();
            JsonNode name = metric.get("name");
            if (name != null) {
                ins.setName(name.asText());
            }
            JsonNode query = metric.get("query");
            if (query.get("tags") == null) {
                query = mapper.readTree(query.asText());
            }
            JsonNode tags = query.get("tags");
            if (tags != null) {
                tags = tags.get("tags");
                if (tags != null) {
                    Iterator<String> fieldNames = tags.fieldNames();
                    while (fieldNames.hasNext()) {
                        String key = fieldNames.next();
                        JsonNode valuenode = tags.get(key);
                        ins.addTag(key, valuenode.asText());
                    }
                }
            }
            setAnnotationLimit(ret, ins, query);
        } else {
            JsonNode start_absolute = node.get("start_absolute");
            JsonNode end_absolute = node.get("end_absolute");
            long now = System.currentTimeMillis();
            if (start_absolute == null && end_absolute == null) {
                ret.setStartAbsolute(0L);
                ret.setEndAbsolute(now);
            } else if (start_absolute != null && end_absolute != null) {
                ret.setStartAbsolute(start_absolute.asLong());
                ret.setEndAbsolute(end_absolute.asLong());
            } else if (start_absolute != null) {
                if (setEndAbsolute(node, ret, start_absolute, now)) {
                    return null;
                }
            } else {
                ret.setEndAbsolute(end_absolute.asLong());
                JsonNode start_relative = node.get("start_relative");
                if (start_relative == null) {
                    ret.setStartAbsolute(0L);
                }
                else {
                    JsonNode value = start_relative.get("value");
                    if (value == null) {
                        return null;
                    }
                    long v = value.asLong();
                    JsonNode unit = start_relative.get("unit");
                    if (unit == null) {
                        return null;
                    }
                    Long time = transTimeFromString(unit.asText());
                    ret.setEndAbsolute(now - v * time);
                }
            }
            JsonNode metrics = node.get("metrics");
            if (metrics != null && metrics.isArray()) {
                for (JsonNode dpnode : metrics) {
                    QueryMetric ins = setQueryMetric(dpnode);
                    setAnnotationLimit(ret, ins, dpnode);
                }
            }
        }
        return ret;
    }

    private void setAnnotationLimit(Query ret, QueryMetric ins, JsonNode query) {
        AnnotationLimit annotationLimit = new AnnotationLimit();
        JsonNode category = query.get("category");
        if (category != null) {
            annotationLimit.setTag(category.asText());
        }
        JsonNode text = query.get("text");
        if (text != null) {
            annotationLimit.setText(text.asText());
        }
        JsonNode description = query.get("description");
        if (description != null) {
            annotationLimit.setTitle(description.asText());
        }
        ins.setAnnotationLimit(annotationLimit);
        ins.setAnnotation(true);
        ret.addQueryMetrics(ins);
    }


    public void addAggregators(QueryMetric q, JsonNode node) {
        JsonNode aggregators = node.get("aggregators");
        if (aggregators == null || !aggregators.isArray()) {
            return;
        }
        for (JsonNode aggregator : aggregators) {
            JsonNode name = aggregator.get("name");
            if (name == null) {
                continue;
            }
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
                    if (sampling == null) {
                        continue;
                    }
                    JsonNode value = sampling.get("value");
                    if (value == null) {
                        continue;
                    }
                    JsonNode unit = sampling.get("unit");
                    if (unit == null) {
                        continue;
                    }
                    long time = transTimeFromString(unit.asText());
                    qa.setDur(value.asLong() * time);
                    break;
                case "div":
                    JsonNode divisor = aggregator.get("divisor");
                    if (divisor == null) {
                        continue;
                    }
                    qa.setDivisor(Double.parseDouble(divisor.asText()));
                    break;
                case "filter":
                    JsonNode filter_op = aggregator.get("filter_op");
                    if (filter_op == null) {
                        continue;
                    }
                    JsonNode threshold = aggregator.get("threshold");
                    if (threshold == null) {
                        continue;
                    }
                    qa.setFilter(new Filter(filter_op.asText(), threshold.asDouble()));
                    break;
                case "save_as":
                    JsonNode metric_name = aggregator.get("metric_name");
                    if (metric_name == null) {
                        continue;
                    }
                    qa.setMetric_name(metric_name.asText());
                    break;
                case "rate":
                    sampling = aggregator.get("sampling");
                    if (sampling == null) {
                        continue;
                    }
                    unit = sampling.get("unit");
                    if (unit == null) {
                        continue;
                    }
                    time = transTimeFromString(unit.asText());
                    qa.setUnit(time);
                    break;
                case "sampler":
                    unit = aggregator.get("unit");
                    if (unit == null) {
                        continue;
                    }
                    time = transTimeFromString(unit.asText());
                    qa.setUnit(time);
                    break;
                case "diff":
                default:
                    break;

            }
            if ("percentile".equals(name.asText())) {
                JsonNode percentile = aggregator.get("percentile");
                if (percentile == null) {
                    continue;
                }
                qa.setPercentile(Double.parseDouble(percentile.asText()));
            }
            q.addAggregator(qa);
        }
    }

    public String parseResultToJson(QueryResult result, boolean isDelete) {
        if (isDelete) {
            return "";
        }
        StringBuilder ret = new StringBuilder("{\"queries\":[");
        for (int i = 0; i < result.getSiz(); i++) {
            ret.append(result.toResultString(i));
            ret.append(",");
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        ret.append("]}");
        return ret.toString();
    }

    public String parseResultToAnnotationJson(QueryResult result, boolean isGrafana) {
        return "[" + result.toAnnotationResultString(isGrafana) +
                "]";
    }

    public String parseResultToGrafanaJson(QueryResult result) {
        StringBuilder ret = new StringBuilder("[");
        for (int i = 0; i < result.getSiz(); i++) {
            ret.append("{");
            ret.append(String.format("\"target\":\"%s\",", result.getQueryMetrics().get(i).getName()));
            ret.append("\"datapoints\":[");
            int n = result.getQueryResultDatasets().get(i).getSize();
            for (int j = 0; j < n; j++) {
                ret.append("[");
                if (result.getQueryResultDatasets().get(i).getValues().get(j) instanceof byte[]) {
                    ret.append(result.getQueryResultDatasets().get(i).getValues().get(j));
                } else {
                    ret.append(result.getQueryResultDatasets().get(i).getValues().get(j).toString());
                }

                ret.append(String.format(",%d", result.getQueryResultDatasets().get(i).getTimestamps().get(j)));
                ret.append("],");
            }
            if (ret.charAt(ret.length() - 1) == ',') {
                ret.deleteCharAt(ret.length() - 1);
            }
            ret.append("]},");
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        ret.append("]");
        return ret.toString();
    }
}
