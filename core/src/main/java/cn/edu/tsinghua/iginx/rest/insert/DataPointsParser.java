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
package cn.edu.tsinghua.iginx.rest.insert;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.*;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.text.html.parser.Entity;
import javax.ws.rs.core.Response;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static cn.edu.tsinghua.iginx.rest.bean.SpecialTime.*;

public class DataPointsParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
    private Reader inputStream = null;
    private final ObjectMapper mapper = new ObjectMapper();
    private List<Metric> metricList = new ArrayList<>();
    private final RestSession session = new RestSession();

    public DataPointsParser() {

    }

    public DataPointsParser(Reader stream) {
        this.inputStream = stream;
    }

    public void parse(boolean isAnnotation) throws Exception {
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error("Error occurred during opening session", e);
            throw e;
        }
        try {
            JsonNode node = mapper.readTree(inputStream);
            if (node.isArray()) {
                for (JsonNode objNode : node) {
                    metricList.add(getMetricObject(objNode, isAnnotation));
                }
            } else {
                metricList.add(getMetricObject(node, isAnnotation));
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing data ", e);
            throw e;
        }
        try {
            sendMetricsData();
        } catch (Exception e) {
            LOGGER.debug("Exception occur for create and send ", e);
            throw e;
        } finally {
            session.closeSession();
        }
    }

    //如果有anno信息会直接放入到插入路径中
    private Metric getMetricObject(JsonNode node, boolean isAnnotation) {
        Metric ret = new Metric();
        ret.setName(node.get("name").asText());
        Iterator<String> fieldNames = node.get("tags").fieldNames();
        Iterator<JsonNode> elements = node.get("tags").elements();
        //insert语句的tag只能有一个val，是否有问题？
        while (elements.hasNext() && fieldNames.hasNext()) {
            ret.addTag(fieldNames.next(), elements.next().textValue());
        }

        // //ANNOEND，扩展，主要是为了后续anntation可以查找到确切路径，加入路径终止符
        // ret.addTag("zEND","END");

        JsonNode tim = node.get("timestamp"), val = node.get("value");
        if (tim != null && val != null) {
            ret.addTimestamp(tim.asLong());
            ret.addValue(val.asText());
        }
        JsonNode dp = node.get("datapoints");
        if (dp != null) {
            if (dp.isArray()) {
                for (JsonNode dpnode : dp) {
                    if (dpnode.isArray()) {
                        ret.addTimestamp(dpnode.get(0).asLong());
                        ret.addValue(dpnode.get(1).asText());
                    }
                }
            }
        }
        JsonNode anno = node.get("annotation");
        if (anno != null) {
            String title=null,description=null;
            JsonNode titleNode = anno.get("title");
            if(titleNode!=null)
                title = titleNode.asText();
            JsonNode dspNode = anno.get("description");
            if(dspNode!=null)
                description = dspNode.asText();
            List<String> category = new ArrayList<>();
            JsonNode categoryNode = anno.get("category");
            if (categoryNode.isArray()) {
                for (JsonNode objNode : categoryNode) {
                    category.add(objNode.asText());
                }
            }

            //将cat的key与val颠倒后作为tag进行插入
            for(String cat : category){
                ret.addTag(cat,"category");
            }
            if(title!=null)
                ret.addAnno("title",title);
            if(description!=null)
                ret.addAnno("description",description);
        }
        return ret;
    }

    public void sendData() {
        try {
            session.openSession();
            sendMetricsData();
        } catch (Exception e) {
            LOGGER.error("Error occurred during sending data ", e);
        }
        session.closeSession();
    }

    public List<Metric> getMetricList() {
        return metricList;
    }

    public void setMetricList(List<Metric> metricList) {
        this.metricList = metricList;
    }

    private Long ifHasAnnoSequence() {
        try {
            //构造查询
            QueryMetric metric = new QueryMetric();
            metric.setName(ANNOTAIONSEQUENCE);
//            metric.addLastAggregator();
            Query query = new Query();
            query.addQueryMetrics(metric);
            query.setStartAbsolute(1L);
            query.setEndAbsolute(2L);

            //执行查询
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute(false);

            //判断是否存在
            if(result.getQueryResultDatasets().get(0).getPaths().isEmpty()) {
                return new Long(-1L);
            } else {
                if(result.getQueryResultDatasets().get(0).getTimestamps().isEmpty())
                    return new Long(ANNOSTARTTIME+1L);
                else {
                    Object val = result.getQueryResultDatasets().get(0).getValueLists().get(0).get(0);
                    String valStr = new String();
                    if (val instanceof byte[]) {
                        valStr = new String((byte[]) val);
                    } else {
                        valStr = String.valueOf(val.toString());
                    }
                    return new Long(Long.parseLong(valStr));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return -1L;
        }
    }

    private void createAnnoSequence(boolean ifUpdate, Long val) throws Exception {
        List<String> paths = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        Object[] valuesList = new Object[1];
        Object[] value = new Object[2];
        List<DataType> type = new ArrayList<>();
        List<Map<String, String>> tagsList = new ArrayList<>();


        paths.add(ANNOTAIONSEQUENCE);
        timestamps.add(1L);
        if(ifUpdate) {
            value[0] = getType(String.valueOf(val),DataType.BINARY);
        } else {
            value[0] = getType(String.valueOf(ANNOSTARTTIME+1L),DataType.BINARY);
        }

        type.add(DataType.BINARY);
        valuesList[0] = value;
        try {
            session.insertNonAlignedColumnRecords(paths, timestamps.stream().mapToLong(Long::longValue).toArray(), valuesList, type, null);
        } catch (ExecutionException e) {
            LOGGER.error("Error occurred during insert ", e);
            throw e;
        }
    }

    private void insertAnnoSquence(Map<Long,String> annoSequence) throws Exception {
        List<Long> timestamps = new ArrayList<>();
        List<String> ANNOPATHS = new ArrayList<>();
        Object[] valuesList = new Object[1];
        Object[] valuesAnno = new Object[2];
        List<DataType> type = new ArrayList<>();

        int pos = 0;
        for(Map.Entry<Long,String> entry : annoSequence.entrySet()) {
            timestamps.add(entry.getKey());
            valuesAnno[pos++] = getType(String.valueOf(entry.getValue()),DataType.BINARY);
        }
        valuesList[0] = valuesAnno;
        type.add(DataType.BINARY);
        ANNOPATHS.add(ANNOTAIONSEQUENCE);
        try {
            session.insertNonAlignedColumnRecords(ANNOPATHS, timestamps.stream().mapToLong(Long::longValue).toArray(), valuesList, type, null);
        } catch (ExecutionException e) {
            LOGGER.error("Error occurred during insert ", e);
            throw e;
        }
    }
    private void insertAnno(List<String> paths, List<Map<String, String>> tagsList, Map<String,String> anno) throws Exception {
        //首先判断是否存在TitleDsp序列，并获取要插入的时间戳
        Long time = ifHasAnnoSequence();
        if(!time.equals(-1L)) {
            Object[] valuesList = new Object[1];
            Object[] valuesAnno = new Object[2];
            List<DataType> type = new ArrayList<>();
            List<Long> timestamps = new ArrayList<>();
            Map<Long,String> annoSequence = new TreeMap<>();
            Long num = 0L;

            int pos = 0;
            if(anno.get("title")!=null){
                valuesAnno[pos++] = getType(String.valueOf(time+num),DataType.DOUBLE);
                annoSequence.put(time+num,anno.get("title"));
                timestamps.add(TITLETIEM);
                num++;
            }
            if(anno.get("description")!=null){
                valuesAnno[pos++] = getType(String.valueOf(time+num),DataType.DOUBLE);
                annoSequence.put(time+num,anno.get("description"));
                timestamps.add(DESCRIPTIONTIEM);
                num++;
            }
            //首先更新anno列表可用最小值
            createAnnoSequence(true, time+num);
            //在anno列表中插入title以及dsp信息
            insertAnnoSquence(annoSequence);

            //在原序列中插入相应的时间戳值
            valuesList[0] = valuesAnno;
            type.add(DataType.DOUBLE);
            try {
                session.insertNonAlignedColumnRecords(paths, timestamps.stream().mapToLong(Long::longValue).toArray(), valuesList, type, tagsList);
            } catch (ExecutionException e) {
                LOGGER.error("Error occurred during insert ", e);
                throw e;
            }
        } else {
            createAnnoSequence(false, null);
            insertAnno(paths,tagsList,anno);
        }
    }

    private void sendMetricsData() throws Exception {
        for (Metric metric : metricList) {
            List<Map<String, String>> tagsList = new ArrayList<>();
            tagsList.add(metric.getTags());

            StringBuilder path = new StringBuilder();
            path.append(metric.getName());
            List<String> paths = new ArrayList<>();
            paths.add(path.toString());

            int size = metric.getTimestamps().size();
            List<DataType> type = new ArrayList<>();
            type.add(findType(metric.getValues()));

            Object[] valuesList = new Object[1];
            Object[] values = new Object[size];
            for (int i = 0; i < size; i++) {
                values[i] = getType(metric.getValues().get(i), type.get(0));
            }
            valuesList[0] = values;
            try {
                session.insertNonAlignedColumnRecords(paths, metric.getTimestamps().stream().mapToLong(Long::longValue).toArray(), valuesList, type, tagsList);
                if (!metric.getAnno().isEmpty()) {
                    insertAnno(paths,tagsList,metric.getAnno());
                }
            } catch (ExecutionException e) {
                LOGGER.error("Error occurred during insert ", e);
                throw e;
            }
        }
    }

    private Map<String, String> getTagsFromPaths(String path, StringBuilder name) {//LHZ确认下是否传入了引用
        Map<String, String> ret = new TreeMap<>();//LHZ这里要再次确认下tag的顺序是否和底层存储一样
        int firstBrace = path.indexOf("{");
        int lastBrace = path.indexOf("}");
        if(firstBrace==-1 || lastBrace==-1){
            name.append(path);
            return ret;
        }
        name.append(path.substring(0, firstBrace));
        String tagLists = path.substring(firstBrace+1, lastBrace);
        String[] splitpaths = tagLists.split(",");
        for(String tag : splitpaths){
            int equalPos = tag.indexOf("=");
            String tagKey = tag.substring(0, equalPos);
            String tagVal = tag.substring(equalPos+1);
            ret.put(tagKey,tagVal);
        }

        // //ANNOEND终止符扩展
        // ret.put("zEND","END");
        return ret;
    }

    //LHZ注意了！！给路径中添加path，这个是允许的，但是一定要保证顺序！！
    //这个函数名修改一下
    private String pathAppendAnno(Metric metric, String path, AnnotationLimit annotationLimit){
        StringBuilder name = new StringBuilder();
        Map<String, String> tags = getTagsFromPaths(path, name);
        for(String tag : annotationLimit.getTag()){
            tags.putIfAbsent(tag, "category");
        }
        metric.setTags(tags);
        return name.toString();
    }

    //修改路径，并插入数据
    public void handleAnnotationAppend(Query preQuery, QueryResult preQueryResult) throws Exception {
        //创建session
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error("Error occurred during opening session", e);
            throw e;
        }
        try{
            for (int pos = 0; pos < preQueryResult.getSiz(); pos++) {//LHZ这里在测试时确认是否每个resultDataSet只有一个值

                QueryResultDataset queryResultDataset = preQueryResult.getQueryResultDatasets().get(pos);
                QueryMetric queryBase = preQueryResult.getQueryMetrics().get(pos);
                for(int pl = 0; pl < queryResultDataset.getPaths().size(); pl++) {
                    Metric metric = new Metric();
                    //分析出tag加入到metric中
                    String name = pathAppendAnno(metric, queryResultDataset.getPaths().get(pl), queryBase.getAnnotationLimit());
                    metric.setName(name);
                    //添加anno的title等信息
                    if(!queryBase.getAnnotationLimit().getTitle().equals(".*"))
                        metric.addAnno("title", queryBase.getAnnotationLimit().getTitle());
                    if(!queryBase.getAnnotationLimit().getText().equals(".*"))
                        metric.addAnno("description", queryBase.getAnnotationLimit().getText());
                    //添加数据点信息
                    for(int tl = 0;tl<queryResultDataset.getTimeLists().get(pl).size();tl++){
                        metric.addTimestamp(queryResultDataset.getTimeLists().get(pl).get(tl));
                        metric.addValue(String.valueOf(queryResultDataset.getValueLists().get(pl).get(tl)));
                    }

                    //LHZ以下代码重复了，能否合并到一个函数？？？
                    //执行插入
                    StringBuilder path = new StringBuilder();
                    path.append(metric.getName());
                    List<String> paths = new ArrayList<>();
                    paths.add(path.toString());
                    List<Map<String,String>> taglist = new ArrayList<>();
                    taglist.add(metric.getTags());
                    int size = metric.getTimestamps().size();
                    List<DataType> type = new ArrayList<>();
                    type.add(findType(metric.getValues()));
                    Object[] valuesList = new Object[1];
                    Object[] values = new Object[size];
                    for (int i = 0; i < size; i++) {
                        values[i] = getType(metric.getValues().get(i), type.get(0));
                    }
                    valuesList[0] = values;
                    try {
                        //LHZ 因为我们默认是可以通过加@的路径访问实现确切的插入，所以无需添加tag
                        session.insertNonAlignedColumnRecords(paths, metric.getTimestamps().stream().mapToLong(Long::longValue).toArray(), valuesList, type, taglist);
                        if (!metric.getAnno().isEmpty()) {
                            insertAnno(paths,taglist,metric.getAnno());
                        }
                    } catch (ExecutionException e) {
                        LOGGER.error("Error occurred during insert ", e);
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Exception occur for create and send ", e);
            throw e;
        } finally {
            session.closeSession();
        }
    }

    private Metric updateAnnoPath(String path, AnnotationLimit annoLimit) {
        Metric metric = new Metric();
        StringBuilder name = new StringBuilder();
        Map<String,String> tags = getTagsFromPaths(path, name);
        Map<String,String> newTags = new TreeMap<>();
        for(Map.Entry<String,String> entry : tags.entrySet()) {
            if(!entry.getValue().equals("category"))
                newTags.put(entry.getKey(),entry.getValue());
        }
        if(!annoLimit.getTag().isEmpty())
            for(String tag : annoLimit.getTag()) {
                newTags.putIfAbsent(tag, "category");
            }

        metric.setTags(newTags);
        metric.setName(name.toString());
        return metric;
    }

    private boolean specificAnnoCategoryPath(Map<String, String> tags, AnnotationLimit annoLimit) {
        int num = 0;

        //数量相同就欧克克
        for(Map.Entry<String,String> entry : tags.entrySet()) {
            if(entry.getValue().equals("category")) num++;
        }
        if(num==annoLimit.getTag().size()) return true;
        return false;
    }

    public void handleAnnotationUpdate(Query preQuery, QueryResult preQueryResult) throws Exception {
        //创建session
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error("Error occurred during opening session", e);
            throw e;
        }
        try{
            for (int pos = 0; pos < preQueryResult.getSiz(); pos++) {//LHZ这里在测试时确认是否每个resultDataSet只有一个值
                QueryResultDataset queryResultDataset = preQueryResult.getQueryResultDatasets().get(pos);
                QueryMetric queryBase = preQueryResult.getQueryMetrics().get(pos);
                for(int pl = 0; pl < queryResultDataset.getPaths().size(); pl++){
                    Metric metric = new Metric();
                    StringBuilder name = new StringBuilder();
                    //添加包含@的路径
                    Map<String, String> tags = getTagsFromPaths(queryResultDataset.getPaths().get(pl), name);
                    //这里更新为包含关系2022.8.12.23.24
                    //如果符合category完全符合，则执行后续操作
//                    if(!specificAnnoCategoryPath(tags, preQuery.getQueryMetrics().get(pos).getAnnotationLimit())) continue;
                    //更改为新的anno信息，即将路径中的cat信息更新
                    AnnotationLimit newAnnoLimit = preQuery.getQueryMetrics().get(pos).getNewAnnotationLimit();
                    metric = updateAnnoPath(queryResultDataset.getPaths().get(pl), newAnnoLimit);
                    //添加anno的title等信息
                    if(!queryBase.getNewAnnotationLimit().getTitle().equals(".*"))
                        metric.addAnno("title", queryBase.getNewAnnotationLimit().getTitle());
                    if(!queryBase.getNewAnnotationLimit().getText().equals(".*"))
                        metric.addAnno("description", queryBase.getNewAnnotationLimit().getText());
                    //添加数据点信息
                    for(int tl = 0;tl<queryResultDataset.getTimeLists().get(pl).size();tl++){
                        metric.addTimestamp(queryResultDataset.getTimeLists().get(pl).get(tl));
                        metric.addValue(String.valueOf(queryResultDataset.getValueLists().get(pl).get(tl)));
                    }

                    //LHZ以下代码重复了，能否合并到一个函数？？？
                    //执行插入
                    StringBuilder path = new StringBuilder();
                    path.append(metric.getName());
                    List<String> paths = new ArrayList<>();
                    paths.add(path.toString());
                    List<Map<String,String>> taglist = new ArrayList<>();
                    taglist.add(metric.getTags());
                    int size = metric.getTimestamps().size();
                    List<DataType> type = new ArrayList<>();
                    type.add(findType(metric.getValues()));
                    Object[] valuesList = new Object[1];
                    Object[] values = new Object[size];
                    for (int i = 0; i < size; i++) {
                        values[i] = getType(metric.getValues().get(i), type.get(0));
                    }
                    valuesList[0] = values;
                    try {
                        //LHZ 因为我们默认是可以通过加@的路径访问实现确切的插入，所以无需添加tag
                        session.insertNonAlignedColumnRecords(paths, metric.getTimestamps().stream().mapToLong(Long::longValue).toArray(), valuesList, type, taglist);
                        if (!metric.getAnno().isEmpty()) {
                            insertAnno(paths,taglist,metric.getAnno());
                        }
                    } catch (ExecutionException e) {
                        LOGGER.error("Error occurred during insert ", e);
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.debug("Exception occur for create and send ", e);
            throw e;
        } finally {
            session.closeSession();
        }
    }

    Object getType(String str, DataType tp) {
        switch (tp) {
            case BINARY:
                return str.getBytes();
            case DOUBLE:
                return Double.parseDouble(str);
            default:
                return null;
        }
    }

    DataType findType(List<String> values) {
        for (String value : values) {
            try {
                Double.parseDouble(value);
            } catch (NumberFormatException e) {
                return DataType.BINARY;
            }
        }
        return DataType.DOUBLE;
    }
}