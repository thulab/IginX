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
package cn.edu.tsinghua.iginx.rest.bean;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorType;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Data
public class QueryResult {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryResult.class);
    private static final IMetaManager META_MANAGER = DefaultMetaManager.getInstance();
    private List<QueryMetric> queryMetrics = new ArrayList<>();
    private List<QueryResultDataset> queryResultDatasets = new ArrayList<>();
    private List<QueryAggregator> queryAggregators = new ArrayList<>();
    private int siz = 0;


    private void addQueryMetric(QueryMetric queryMetric) {
        queryMetrics.add(queryMetric);
    }


    public void addqueryResultDataset(QueryResultDataset queryResultDataset) {
        queryResultDatasets.add(queryResultDataset);
    }

    public void addQueryAggregator(QueryAggregator queryAggregator) {
        queryAggregators.add(queryAggregator);
    }

    public void addResultSet(QueryResultDataset queryDataSet, QueryMetric queryMetric, QueryAggregator queryAggregator) {
        addqueryResultDataset(queryDataSet);
        addQueryMetric(queryMetric);
        addQueryAggregator(queryAggregator);
        siz += 1;
    }

    public String toResultString(int num) {
        return "{" + sampleSizeToString(num) +
                "," +
                "\"results\": [{ " +
                nameToString(num) +
                "," +
                groupbyToString() +
                "," +
                tagsToString(num) +
                "," +
                valueToString(num) +
                "}]}";
    }


    public String toAnnotationResultString(boolean isGrafana) {
        StringBuilder ret = new StringBuilder();
        List<Annotation> values = new ArrayList<>();
        int siz = queryResultDatasets.get(0).getValues().size();
        for (int i = 0; i < siz; i++) {
            Annotation ins = new Annotation(new String((byte[]) queryResultDatasets.get(0).getValues().get(i)), queryResultDatasets.get(0).getTimestamps().get(i));
            values.add(ins);
        }
        int now = 0;
        if (siz == 0) {
            return "{}";
        }
        if (isGrafana) {
            for (int i = 1; i < siz; i++) {
                if (values.get(i).isEqual(values.get(i - 1))) {
                    if (values.get(i - 1).match(queryMetrics.get(0).getAnnotationLimit())) {
                        ret.append("{");
                        ret.append(String.format("\"text\": \"%s\",", values.get(i - 1).getText()));
                        ret.append(String.format("\"title\": \"%s\",", values.get(i - 1).getTitle()));
                        ret.append("\"isRegion\": true,");
                        ret.append(String.format("\"time\": \"%d\",", values.get(now).getTimestamp()));
                        ret.append(String.format("\"timeEnd\": \"%d\",", values.get(i - 1).getTimestamp()));
                        ret.append("\"tags\": [");
                        for (String tag : values.get(i - 1).getTags()) {
                            ret.append(String.format("\"%s\",", tag));
                        }
                        if (ret.charAt(ret.length() - 1) == ',') {
                            ret.deleteCharAt(ret.length() - 1);
                        }
                        ret.append("]},");
                    }
                    now = i;
                }
            }
            if (values.get(siz - 1).match(queryMetrics.get(0).getAnnotationLimit())) {
                ret.append("{");
                ret.append(String.format("\"text\": \"%s\",", values.get(siz - 1).getText()));
                ret.append(String.format("\"title\": \"%s\",", values.get(siz - 1).getTitle()));
                ret.append("\"isRegion\": true,");
                ret.append(String.format("\"time\": \"%d\",", values.get(now).getTimestamp()));
                ret.append(String.format("\"timeEnd\": \"%d\",", values.get(siz - 1).getTimestamp()));
                ret.append("\"tags\": [");
                for (String tag : values.get(siz - 1).getTags()) {
                    ret.append(String.format("\"%s\",", tag));
                }
                if (ret.charAt(ret.length() - 1) == ',') {
                    ret.deleteCharAt(ret.length() - 1);
                }
                ret.append("]}");
            }
        } else {
            for (int i = 1; i < siz; i++) {
                if (values.get(i).isEqual(values.get(i - 1))) {
                    if (values.get(i - 1).match(queryMetrics.get(0).getAnnotationLimit())) {
                        ret.append("{");
                        ret.append(String.format("\"text\": \"%s\",", values.get(i - 1).getText()));
                        ret.append(String.format("\"description\": \"%s\",", values.get(i - 1).getTitle()));
                        ret.append(String.format("\"time\": \"%d\",", values.get(now).getTimestamp()));
                        ret.append(String.format("\"timeEnd\": \"%d\",", values.get(i - 1).getTimestamp()));
                        ret.append("\"category\": [");
                        for (String tag : values.get(i - 1).getTags()) {
                            ret.append(String.format("\"%s\",", tag));
                        }
                        if (ret.charAt(ret.length() - 1) == ',') {
                            ret.deleteCharAt(ret.length() - 1);
                        }
                        ret.append("]},");
                    }
                    now = i;
                }
            }
            if (values.get(siz - 1).match(queryMetrics.get(0).getAnnotationLimit())) {
                ret.append("{");
                ret.append(String.format("\"text\": \"%s\",", values.get(siz - 1).getText()));
                ret.append(String.format("\"description\": \"%s\",", values.get(siz - 1).getTitle()));
                ret.append(String.format("\"time\": \"%d\",", values.get(now).getTimestamp()));
                ret.append(String.format("\"timeEnd\": \"%d\",", values.get(siz - 1).getTimestamp()));
                ret.append("\"category\": [");
                for (String tag : values.get(siz - 1).getTags()) {
                    ret.append(String.format("\"%s\",", tag));
                }
                if (ret.charAt(ret.length() - 1) == ',') {
                    ret.deleteCharAt(ret.length() - 1);
                }
                ret.append("]}");
            }
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        return ret.toString();
    }

    private String nameToString(int num) {
        if (queryAggregators.get(num).getType() == QueryAggregatorType.SAVE_AS) {
            return String.format("\"name\": \"%s\"", queryAggregators.get(num).getMetric_name());
        } else {
            return String.format("\"name\": \"%s\"", queryMetrics.get(num).getName());
        }
    }

    private String groupbyToString() {
        return "\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}]";
    }

    private String tagsToString(int num) {
        StringBuilder ret = new StringBuilder(" \"tags\": {");
        Map<String, List<String>> tags = null;
        try {
            tags = getTagsFromPaths(queryMetrics.get(num).getName(),
                    queryResultDatasets.get(num).getPaths());
        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing tags ", e);

        }
        for (Map.Entry<String, List<String>> entry : tags.entrySet()) {
            ret.append(String.format("\"%s\": [", entry.getKey()));
            for (String v : entry.getValue()) {
                ret.append(String.format("\"%s\",", v));
            }
            ret.deleteCharAt(ret.length() - 1);
            ret.append("],");
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        ret.append("}");
        return ret.toString();
    }

    private String valueToString(int num) {
        StringBuilder ret = new StringBuilder(" \"values\": [");
        int n = queryResultDatasets.get(num).getSize();
        for (int i = 0; i < n; i++) {
            ret.append(String.format("[%d,", queryResultDatasets.get(num).getTimestamps().get(i)));
            if (queryResultDatasets.get(num).getValues().get(i) instanceof byte[]) {
                ret.append(new String((byte[]) queryResultDatasets.get(num).getValues().get(i)));
            }
            else {
                ret.append(queryResultDatasets.get(num).getValues().get(i).toString());
            }
            ret.append("],");
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        ret.append("]");
        return ret.toString();
    }

    private String sampleSizeToString(int num) {
        return "\"sample_size\": " + queryResultDatasets.get(num).getSampleSize();
    }

    private Map<String, List<String>> getTagsFromPaths(String name, List<String> paths) throws Exception {
        List<Map<String, Integer>> dup = new ArrayList<>();
        Map<String, List<String>> ret = new TreeMap<>();
        Map<Integer, String> pos2path = new TreeMap<>();
        Map<String, Integer> metricschema = META_MANAGER.getSchemaMapping(name);
        if (metricschema == null) {
            throw new Exception("No metadata found");
        } else {
            for (Map.Entry<String, Integer> entry : metricschema.entrySet()) {
                pos2path.put(entry.getValue(), entry.getKey());
                dup.add(new HashMap<>());
            }
        }
        for (String path : paths) {
            String[] splitpaths = path.split("\\.");
            for (int i = 0; i < pos2path.size(); i++) {
                if (dup.get(i).get(splitpaths[i]) == null) {
                    dup.get(i).put(splitpaths[i], 1);
                    ret.computeIfAbsent(pos2path.get(i + 1), k -> new ArrayList<>());
                    ret.get(pos2path.get(i + 1)).add(splitpaths[i]);
                }
            }

        }
        return ret;
    }
}
