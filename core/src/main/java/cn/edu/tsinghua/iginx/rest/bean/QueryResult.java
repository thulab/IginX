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
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorType;
import cn.edu.tsinghua.iginx.rest.RestUtils;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static cn.edu.tsinghua.iginx.rest.RestUtils.TOPTIEM;

@Data
public class QueryResult {
    public static final Logger LOGGER = LoggerFactory.getLogger(QueryResult.class);
    private static final IMetaManager META_MANAGER = DefaultMetaManager.getInstance();
    private List<QueryMetric> queryMetrics = new ArrayList<>();
    private List<QueryResultDataset> queryResultDatasets = new ArrayList<>();
    private List<QueryAggregator> queryAggregators = new ArrayList<>();
    private int siz = 0;


    public void addQueryMetric(QueryMetric queryMetric) {
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

    public void addResultSet(QueryResultDataset queryDataSet) {
        addqueryResultDataset(queryDataSet);
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

    public String toResultStringAnno(int now, int pos) {
        return "{"+
                nameToString(pos) +
                "," +
                tagsToStringAnno(queryResultDatasets.get(pos).getPaths().get(now)) +
                "," +
                annoToString(now,pos) +
                "}";
    }

    public String toResultString(int now, int pos) {
        return "{"+
                nameToString(pos) +
                "," +
                tagsToStringAnno(queryResultDatasets.get(pos).getPaths().get(now)) +
                "," +
                annoDataToString(now,pos) +
                "," +
                valueToStringAnno(now,pos) +
                "}";
    }

    public String toAnnotationResultString(QueryResult anno, boolean isGrafana) {
        StringBuilder ret = new StringBuilder();
        List<Annotation> values = new ArrayList<>();
        int siz = queryResultDatasets.get(0).getValues().size();
//        for (int i = 0; i < siz; i++) {
//            Annotation ins = new Annotation(new String((byte[]) queryResultDatasets.get(0).getValues().get(i)), queryResultDatasets.get(0).getTimestamps().get(i));
//            values.add(ins);
//        }
        int now = 0;
        if (siz == 0) {
            return "{}";
        }
        if (isGrafana) {
            for (int i = 1; i < siz; i++) {
                if (values.get(i).isEqual(values.get(i - 1))) {
                    if (values.get(i - 1).match(queryMetrics.get(0).getAnnotationLimit())) {
                        buildGrafanaString(ret, values, i, now);
                        ret.append("]},");
                    }
                    now = i;
                }
            }
            if (values.get(siz - 1).match(queryMetrics.get(0).getAnnotationLimit())) {
                buildGrafanaString(ret, values, siz, now);
                ret.append("]}");
            }
        } else {
            for (int i = 0; i < siz; i++) {
                for(int j=0; j<queryResultDatasets.get(i).getPaths().size(); j++){
                    buildAnnotationString(ret, anno, j, i);
                    ret.append("},");
                }
            }
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        return ret.toString();
    }

    //从包含cat的完整路径中获取tags{}
    private String tagsToStringAnno(String path) {
        StringBuilder ret = new StringBuilder(" \"tags\": {");
        QueryParser parser = new QueryParser();
        Map<String,String> tags = parser.getTagsFromPaths(path, new StringBuilder());
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if(!entry.getValue().equals(RestUtils.CATEGORY)) {
                ret.append("\"" + entry.getKey() + "\" : [\"" + entry.getValue() + "\"],");
            }
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        ret.append("}");
        return ret.toString();
    }

    //获取anno信息{}
    private String annoToString(int now, int i) {
        StringBuilder ret = new StringBuilder("\"annotation\": {");
        ret.append(String.format("\"title\": \"%s\",", queryResultDatasets.get(i).getTitles().get(now)));
        ret.append(String.format("\"description\": \"%s\",", queryResultDatasets.get(i).getDescriptions().get(now)));
        ret.append("\"category\": [");
        for (String tag : queryResultDatasets.get(i).getCategorys().get(now)) {
            ret.append(String.format("\"%s\",", tag));
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        ret.append("]}");
        return ret.toString();
    }

    //获取anno信息{}
    private String annoDataToString(int now, int i) {
        StringBuilder ret = new StringBuilder("\"annotation\": {");
        ret.append(String.format("\"title\": \"%s\",", queryResultDatasets.get(i).getTitles().get(now)));
        ret.append(String.format("\"description\": \"%s\",", queryResultDatasets.get(i).getDescriptions().get(now)));
        ret.append("\"category\": [");
        for (String tag : queryResultDatasets.get(i).getCategorys().get(now)) {
            ret.append(String.format("\"%s\",", tag));
        }
        if (ret.charAt(ret.length() - 1) == ',') {
            ret.deleteCharAt(ret.length() - 1);
        }
        ret.append("]}");
        return ret.toString();
    }

    private void buildAnnotationString(StringBuilder ret, QueryResult anno, int now, int i) {
        ret.append("{");
        ret.append(String.format("\"name\": \"%s\",", queryMetrics.get(i).getName()));
        ret.append(tagsToStringAnno(queryMetrics.get(i).getName()));
        annoDataToString(now,i);
    }

    private void buildGrafanaString(StringBuilder ret, List<Annotation> values, int siz, int now) {
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
        Map<String, Set<String>> tags = null;
        try {
            tags = getTagsFromPaths(queryMetrics.get(num).getName(),
                queryResultDatasets.get(num).getPaths());
        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing tags ", e);

        }
        for (Map.Entry<String, Set<String>> entry : tags.entrySet()) {
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
            long timeRes = TimeUtils.getTimeFromNsToSpecPrecision(queryResultDatasets.get(num).getTimestamps().get(i), TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
            ret.append(String.format("[%d,", timeRes));
            if (queryResultDatasets.get(num).getValues().get(i) instanceof byte[]) {
                ret.append("\"");
                ret.append(new String((byte[]) queryResultDatasets.get(num).getValues().get(i)));
                ret.append("\"");
            } else {
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

    private String valueToStringAnno(int now, int num) {
        StringBuilder ret = new StringBuilder(" \"values\": [");
        List<Long> timeLists = queryResultDatasets.get(num).getTimeLists().get(now);
        List<Object> valueLists = queryResultDatasets.get(num).getValueLists().get(now);

        for (int j = 0; j < timeLists.size(); j++) {
            if (timeLists.get(j) > TOPTIEM) continue;
            long timeInPrecision = TimeUtils.getTimeFromNsToSpecPrecision(timeLists.get(j), TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
            ret.append(String.format("[%d,", timeInPrecision));
            if (valueLists.get(j) instanceof byte[]) {
                ret.append("\"");
                ret.append(new String((byte[]) valueLists.get(j)));
                ret.append("\"");
            } else {
                ret.append(valueLists.get(j).toString());
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

    private Map<String, Set<String>> getTagsFromPaths(String name, List<String> paths) throws Exception {
        List<Map<String, Integer>> dup = new ArrayList<>();
        Map<String, Set<String>> ret = new TreeMap<>();
        Map<Integer, String> pos2path = new TreeMap<>();
        for (String path : paths) {
            int firstBrace = path.indexOf("{");
            int lastBrace = path.indexOf("}");
            if(firstBrace==-1 || lastBrace==-1) break;
            String tagLists = path.substring(firstBrace+1, lastBrace);
            String[] splitpaths = tagLists.split(",");
            for(String tag : splitpaths){
                int equalPos = tag.indexOf("=");
                String tagKey = tag.substring(0, equalPos);
                String tagVal = tag.substring(equalPos+1);
                ret.computeIfAbsent(tagKey, k -> new HashSet<String>());
                ret.get(tagKey).add(tagVal);
            }
        }
        return ret;
    }
}