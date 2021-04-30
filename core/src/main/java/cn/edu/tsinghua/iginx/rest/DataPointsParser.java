package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import st4hidden.org.antlr.runtime.tree.Tree;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;


import static com.google.common.base.Preconditions.checkNotNull;

public class DataPointsParser
{
    private final IMetaManager metaManager = SortedListAbstractMetaManager.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private final Reader inputStream;
    private final Gson gson;
    private ObjectMapper mapper = new ObjectMapper();
    private List<Metric> metricList = new ArrayList<>();
    private Session session = new Session("127.0.0.1", 6324, "root", "root");
    private Map<String, Map<String,Integer>> schemamapping = MetricsResource.schemamapping;

    public DataPointsParser(Reader stream, Gson gson)
    {
        this.inputStream = stream;
        this.gson = gson;
    }

    public ValidationErrors parse() throws IOException
    {
        try
        {
            session.openSession();
        }
        catch (SessionException e)
        {
            e.printStackTrace();
        }
        ValidationErrors validationErrors = new ValidationErrors();
        try
        {
            JsonNode node = mapper.readTree(inputStream);
            if (node.isArray())
            {
                for (JsonNode objNode : node)
                {
                    metricList.add(getMetricObject(objNode));
                }
            }
            else
            {
                metricList.add(getMetricObject(node));
            }

        }
        catch (Exception e)
        {
            validationErrors.addErrorMessage("Invalid json. No content due to end of input.");
        }
        try
        {
            sendMetricsData();
        }
        catch (Exception e)
        {
            LOGGER.debug("Exception occur for create and send:,", e);
        }
        return validationErrors;
    }

    private Metric getMetricObject(JsonNode node)
    {
        Metric ret = new Metric();
        ret.setName(node.get("name").asText());
        Iterator<String> fieldNames = node.get("tags").fieldNames();
        Iterator<JsonNode> elements = node.get("tags").elements();
        while (elements.hasNext() && fieldNames.hasNext())
        {
            ret.addTag(fieldNames.next(), elements.next().textValue());
        }
        JsonNode tim = node.get("timestamp"), val = node.get("value");
        if (tim != null && val != null)
        {
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
                    if (dpnode.isArray())
                    {
                        ret.addTimestamp(dpnode.get(0).asLong());
                        ret.addValue(dpnode.get(1).asText());
                    }
                }
            }
        }
        return ret;
    }

    private void sendMetricsData()
    {
        for (Metric metric: metricList)
        {
            boolean needUpdate = false;
            Map<String, Integer> metricschema = schemamapping.get(metric.getName());
            if (metricschema == null)
            {
                needUpdate = true;
                metricschema = metaManager.getSchemaMapping(metric.getName());
                if (metricschema == null)
                {
                    metricschema = new ConcurrentHashMap<>();
                }
                schemamapping.put(metric.getName(),metricschema);
            }
            Iterator iter = metric.getTags().entrySet().iterator();
            while (iter.hasNext())
            {
                Map.Entry entry = (Map.Entry) iter.next();
                if (metricschema.get(entry.getKey()) == null)
                {
                    needUpdate = true;
                    int pos = metricschema.size() + 1;
                    metricschema.put((String) entry.getKey(), pos);
                }
            }
            if (needUpdate)
                metaManager.addOrUpdateSchemaMapping(metric.getName(), metricschema);
            Map<Integer, String> pos2path = new TreeMap<>();
            for (Map.Entry<String, Integer> entry: metricschema.entrySet())
                pos2path.put(entry.getValue(), entry.getKey());
            StringBuilder path = new StringBuilder("");
            iter = pos2path.entrySet().iterator();
            while (iter.hasNext())
            {
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
            for (int i = 0; i < size; i++)
            {
                values[i] = getType(metric.getValues().get(i), type.get(0));
            }
            valuesList[0] = values;
            try
            {
                session.insertColumnRecords(paths,metric.getTimestamps().stream().mapToLong(t->t.longValue()).toArray(),valuesList,type,null);
            }
            catch (SessionException e)
            {
                e.printStackTrace();
            }
            catch (ExecutionException e)
            {
                e.printStackTrace();
            }
        }
    }

    Object getType(String str, DataType tp)
    {
        switch (tp)
        {
            case BINARY:
                return str.getBytes();
            case DOUBLE:
                return Double.parseDouble(str);
        }
        return null;
    }

    DataType findType(List<String> values)
    {
        for (int i=0; i<values.size(); i++)
        {
            try
            {
                Double.parseDouble(values.get(i));
            }
            catch (NumberFormatException e)
            {
                return DataType.BINARY;
            }
        }
        return DataType.DOUBLE;
    }
}