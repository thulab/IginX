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

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.rest.bean.Metric;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DataPointsParser {
    public static final String ANNOTATION_SPLIT_STRING = "@@annotation";
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
    private final IMetaManager metaManager = DefaultMetaManager.getInstance();
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private Reader inputStream = null;
    private final ObjectMapper mapper = new ObjectMapper();
    private List<Metric> metricList = new ArrayList<>();
    private final RestSession session = new RestSession();
    private Map<TimeAndPrefixPath, Map<String, String>> batchMap = new HashMap<>();
    private int restReqSplitNum = config.getRestReqSplitNum();



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
        // sub tread execute and await.
        LOGGER.info(String.format("restReqSplitNum: %s", restReqSplitNum));

        if (restReqSplitNum > 1) {
            long batchInsertStartTime = System.currentTimeMillis();
            List<List<Metric>> splitMetricList = averageAssign(metricList, restReqSplitNum);
            CountDownLatch latch = new CountDownLatch(restReqSplitNum);
            for (List<Metric> list : splitMetricList) {
                SenderManager.getInstance().addSender(new Sender(latch, list));
            }
            try {
                latch.await(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                LOGGER.error("Request partial sub threads time out");
            }
            long batchInsertEndTime = System.currentTimeMillis();
            LOGGER.info(String.format("Batch insert cost time: %s ms", batchInsertEndTime - batchInsertStartTime));
        } else {
            try {
                if (isAnnotation) {
                    sendAnnotationMetricsData();
                } else {
                    sendMetricsDataInBatch();
                }
            } catch (Exception e) {
                LOGGER.debug("Exception occur for create and send ", e);
                throw e;
            } finally {
                session.closeSession();
            }
        }
    }

    private Metric getMetricObject(JsonNode node, boolean isAnnotation) {
        Metric ret = new Metric();
        ret.setName(node.get("name").asText());
        Iterator<String> fieldNames = node.get("tags").fieldNames();
        Iterator<JsonNode> elements = node.get("tags").elements();
        while (elements.hasNext() && fieldNames.hasNext()) {
            ret.addTag(fieldNames.next(), elements.next().textValue());
        }
        JsonNode tim = node.get("timestamp"), val = node.get("value");
        if (tim != null && val != null) {
            ret.addTimestamp(tim.asLong());
            ret.addValue(val.asText());
        }
        JsonNode dp = node.get("datapoints");
        if (dp != null) {
            if (dp.isArray()) {
                for (JsonNode dpnode : dp) {
                    if (isAnnotation) {
                        ret.addTimestamp(dpnode.asLong());
                    } else if (dpnode.isArray()) {
                        ret.addTimestamp(dpnode.get(0).asLong());
                        ret.addValue(dpnode.get(1).asText());
                    }
                }
            }
        }
        JsonNode anno = node.get("annotation");
        if (anno != null) {
            ret.setAnnotation(anno.toString().replace("\n", "")
                    .replace("\t", "").replace(" ", ""));
        }
        return ret;
    }

    public void sendData() {
        try {
            session.openSession();
            sendMetricsDataInBatch();
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


    private void sendAnnotationMetricsData() throws Exception {
        for (Metric metric : metricList) {
            boolean needUpdate = false;
            Map<String, Integer> metricschema = metaManager.getSchemaMapping(metric.getName());
            if (metricschema == null) {
                needUpdate = true;
                metricschema = new ConcurrentHashMap<>();
            }
            Iterator iter = metric.getTags().entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                if (metricschema.get(entry.getKey()) == null) {
                    needUpdate = true;
                    int pos = metricschema.size() + 1;
                    metricschema.put((String) entry.getKey(), pos);
                }
            }
            if (needUpdate) {
                metaManager.addOrUpdateSchemaMapping(metric.getName(), metricschema);
            }
            Map<Integer, String> pos2path = new TreeMap<>();
            for (Map.Entry<String, Integer> entry : metricschema.entrySet()) {
                pos2path.put(entry.getValue(), entry.getKey());
            }
            StringBuilder path = new StringBuilder();
            iter = pos2path.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String ins = metric.getTags().get(entry.getValue());
                if (ins != null) {
                    path.append(ins).append(".");
                }
                else {
                    path.append("null.");
                }
            }
            path.append(metric.getName());
            path.append(ANNOTATION_SPLIT_STRING);
            List<String> paths = new ArrayList<>();
            paths.add(path.toString());
            List<DataType> type = new ArrayList<>();
            type.add(DataType.BINARY);
            int size = metric.getTimestamps().size();
            Object[] valuesList = new Object[1];
            Object[] values = new Object[size];
            for (int i = 0; i < size; i++) {
                values[i] = metric.getAnnotation().getBytes();
            }
            valuesList[0] = values;
            session.insertNonAlignedColumnRecords(paths, metric.getTimestamps().stream().mapToLong(Long::longValue).toArray(), valuesList, type, null);
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

    private static <T> List<List<T>> averageAssign(List<T> source, int n) {
        List<List<T>> result = new ArrayList<>();

        int remainder = source.size() % n;
        int number = source.size() / n;
        int offset = 0;

        for (int i = 0; i < n; i++) {
            List<T> value;
            if (remainder > 0) {
                value = source.subList(i * number + offset, (i + 1) * number + offset + 1);
                remainder--;
                offset++;
            } else {
                value = source.subList(i * number + offset, (i + 1) * number + offset);
            }
            result.add(value);
        }
        return result;
    }

    private void sendMetricsDataInBatch() {
        long umamdTime = System.currentTimeMillis();
        LOGGER.info(String.format("Going in to meta data updates"));
        updateMetaAndMergeData();
        LOGGER.info(String.format("MetaData cost time: %s ms", System.currentTimeMillis() - umamdTime));

        for (Map.Entry<TimeAndPrefixPath, Map<String, String>> entry : batchMap.entrySet()) {
            List<String> paths = new ArrayList<>();
            List<DataType> types = new ArrayList<>();
            Object[] values = new Object[1];
            long[] timestamps = new long[1];

            String prefixPath = entry.getKey().getPrefixPath();
            long timestamp = entry.getKey().getTimestamp();
            List<Object> valueList = new ArrayList<>();
            timestamps[0] = timestamp;

            for (Map.Entry<String, String> subEntry : entry.getValue().entrySet()) {
                String suffixPath = subEntry.getKey();
                String value = subEntry.getValue();

                DataType type = findType(new ArrayList<>(Collections.singletonList(value)));
                types.add(type);
                paths.add(prefixPath + suffixPath);
                valueList.add(getType(value, type));
            }

            values[0] = valueList.toArray();

            try {
                long sessionInsertStartTime =  System.currentTimeMillis();
//                session.insertNonAlignedRowRecords(paths, timestamps, values, types, null);
                session.insertRowRecords(paths, timestamps, values, types, null);
                long sessionInsertEndTime =  System.currentTimeMillis();
                LOGGER.info(String.format("Session insert cost time: %s ms", sessionInsertEndTime - sessionInsertStartTime));
            } catch (Exception e) {
                LOGGER.error("Error occurred during insert ", e);
            }
        }
    }

    private void updateMetaAndMergeData() {
        for (Metric metric : metricList) {
            // update meta
            boolean needUpdate = false;
            Map<String, Integer> metricschema = metaManager.getSchemaMapping(metric.getName());
            if (metricschema == null) {
                needUpdate = true;
                metricschema = new ConcurrentHashMap<>();
            }
            Iterator iter = metric.getTags().entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                if (metricschema.get(entry.getKey()) == null) {
                    needUpdate = true;
                    int pos = metricschema.size() + 1;
                    metricschema.put((String) entry.getKey(), pos);
                }
            }
            if (needUpdate) {
                metaManager.addOrUpdateSchemaMapping(metric.getName(), metricschema);
            }
            Map<Integer, String> pos2path = new TreeMap<>();
            for (Map.Entry<String, Integer> entry : metricschema.entrySet()) {
                pos2path.put(entry.getValue(), entry.getKey());
            }
            StringBuilder path = new StringBuilder("");
            iter = pos2path.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String ins = metric.getTags().get(entry.getValue());
                if (ins != null) {
                    path.append(ins + ".");
                }
                else {
                    path.append("null.");
                }
            }
            // merge data in time and prefix path
            String prefixPath = path.toString();
            for (int i = 0; i < metric.getTimestamps().size(); i++) {
                long timestamp = metric.getTimestamps().get(i);
                String value = metric.getValues().get(i);
                TimeAndPrefixPath tpKey = new TimeAndPrefixPath(timestamp, prefixPath);
                if (batchMap.containsKey(tpKey)) {
                    batchMap.get(tpKey).put(metric.getName(), value);
                } else {
                    Map<String, String> metricValueMap = new HashMap<>();
                    metricValueMap.put(metric.getName(), value);
                    batchMap.put(tpKey, metricValueMap);
                }

                if (metric.getAnnotation() != null) {
                    if (batchMap.containsKey(tpKey)) {
                        batchMap.get(tpKey).put(metric.getName() + ANNOTATION_SPLIT_STRING,
                                Arrays.toString(metric.getAnnotation().getBytes()));
                    } else {
                        Map<String, String> metricValueMap = new HashMap<>();
                        metricValueMap.put(metric.getName() + ANNOTATION_SPLIT_STRING, value);
                        batchMap.put(tpKey, metricValueMap);
                    }
                }
            }
        }
    }
}