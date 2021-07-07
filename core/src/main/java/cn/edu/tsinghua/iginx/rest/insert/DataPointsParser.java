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
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

public class DataPointsParser {
    public static final String ANNOTATION_SPLIT_STRING = "@@annotation";
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private final IMetaManager metaManager = DefaultMetaManager.getInstance();
    private Reader inputStream = null;
    private ObjectMapper mapper = new ObjectMapper();
    private List<Metric> metricList = new ArrayList<>();
    private RestSession session = new RestSession();

    public DataPointsParser() {

    }

    public DataPointsParser(Reader stream) {
        this.inputStream = stream;
    }

    public void parse() throws Exception
    {
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
                    metricList.add(getMetricObject(objNode));
                }
            } else {
                metricList.add(getMetricObject(node));
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

    public void parseAnnotation() throws Exception
    {
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
                    metricList.add(getAnnotationMetricObject(objNode));
                }
            } else {
                metricList.add(getAnnotationMetricObject(node));
            }

        } catch (Exception e) {
            LOGGER.error("Error occurred during parsing data ", e);
            throw e;
        }
        try {
            sendAnnotationMetricsData();
        } catch (Exception e) {
            LOGGER.debug("Exception occur for create and send ", e);
            throw e;
        } finally {
            session.closeSession();
        }
    }

    private Metric getAnnotationMetricObject(JsonNode node) {
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
        if (dp != null)
        {
            if (dp.isArray())
            {
                for (JsonNode dpnode : dp)
                {
                    ret.addTimestamp(dpnode.asLong());
                }
            }
        }
        JsonNode anno = node.get("annotation");
        if (anno != null)
        {
            ret.setAnnotation(anno.toString().replace("\n", "")
                    .replace("\t", "").replace(" ", ""));
        }
        return ret;
    }


    private Metric getMetricObject(JsonNode node) {
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
                    if (dpnode.isArray()) {
                        ret.addTimestamp(dpnode.get(0).asLong());
                        ret.addValue(dpnode.get(1).asText());
                    }
                }
            }
        }
        JsonNode anno = node.get("annotation");
        if (anno != null)
        {
            ret.setAnnotation(anno.toString().replace("\n", "")
                    .replace("\t", "").replace(" ", ""));
        }
        return ret;
    }

    public void sendData() {
        try {
            session.openSession();
            sendMetricsData();
        }
        catch (Exception e)
        {
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

    private void sendMetricsData() throws Exception
    {
        for (Metric metric: metricList)
        {
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
            if (needUpdate)
                metaManager.addOrUpdateSchemaMapping(metric.getName(), metricschema);
            Map<Integer, String> pos2path = new TreeMap<>();
            for (Map.Entry<String, Integer> entry : metricschema.entrySet())
                pos2path.put(entry.getValue(), entry.getKey());
            StringBuilder path = new StringBuilder("");
            iter = pos2path.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String ins = metric.getTags().get(entry.getValue());
                if (ins != null)
                    path.append(ins + ".");
                else
                    path.append("null.");
            }
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
                session.insertColumnRecords(paths, metric.getTimestamps().stream().mapToLong(t -> t.longValue()).toArray(), valuesList, type, null);
                if (metric.getAnnotation() != null)
                {
                    for (int i = 0; i < size; i++) {
                        values[i] = metric.getAnnotation().getBytes();
                    }
                    valuesList[0] = values;
                    path.append(ANNOTATION_SPLIT_STRING);
                    paths.set(0, path.toString());
                    type.set(0, DataType.BINARY);
                    session.insertColumnRecords(paths, metric.getTimestamps().stream().mapToLong(t -> t.longValue()).toArray(), valuesList, type, null);
                }
            } catch (ExecutionException e) {
                LOGGER.error("Error occurred during insert ", e);
                throw e;
            }
        }
    }

    private void sendAnnotationMetricsData() throws Exception
    {
        for (Metric metric: metricList)
        {
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
            if (needUpdate)
                metaManager.addOrUpdateSchemaMapping(metric.getName(), metricschema);
            Map<Integer, String> pos2path = new TreeMap<>();
            for (Map.Entry<String, Integer> entry : metricschema.entrySet())
                pos2path.put(entry.getValue(), entry.getKey());
            StringBuilder path = new StringBuilder("");
            iter = pos2path.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                String ins = metric.getTags().get(entry.getValue());
                if (ins != null)
                    path.append(ins + ".");
                else
                    path.append("null.");
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
            for (int i = 0; i < size; i++)
            {
                values[i] = metric.getAnnotation().getBytes();
            }
            valuesList[0] = values;
            session.insertColumnRecords(paths, metric.getTimestamps().stream().mapToLong(t -> t.longValue()).toArray(), valuesList, type, null);
        }
    }


    Object getType(String str, DataType tp) {
        switch (tp) {
            case BINARY:
                return str.getBytes();
            case DOUBLE:
                return Double.parseDouble(str);
        }
        return null;
    }

    DataType findType(List<String> values) {
        for (int i = 0; i < values.size(); i++) {
            try {
                Double.parseDouble(values.get(i));
            } catch (NumberFormatException e) {
                return DataType.BINARY;
            }
        }
        return DataType.DOUBLE;
    }
}