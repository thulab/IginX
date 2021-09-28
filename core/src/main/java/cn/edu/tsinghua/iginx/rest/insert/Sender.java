package cn.edu.tsinghua.iginx.rest.insert;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.rest.RestSession;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class Sender extends Thread {

    private CountDownLatch latch;
    public static final String ANNOTATION_SPLIT_STRING = "@@annotation";
    private static final Logger LOGGER = LoggerFactory.getLogger(DataPointsParser.class);
    private final IMetaManager metaManager = DefaultMetaManager.getInstance();
    private RestSession session;
    private List<Metric> metricList = new ArrayList<>();
    private Map<TimeAndPrefixPath, Map<String, String>> batchMap = new HashMap<>();

    public Sender(CountDownLatch latch, List<Metric> list) {
        this.latch = latch;
        this.metricList.addAll(list);
        this.session = new RestSession();
    }

    @Override
    public void run() {
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error("Error occurred during opening session", e);
        }
        try {
            sendMetricsDataInBatch();
        } catch (Exception e) {
            LOGGER.error("Error occurred during sending data", e);
        } finally {
            latch.countDown();
//            session.closeSession();
        }
    }

    public void sendMetricsDataInBatch() throws Exception {
        updateMetaAndMergeData();
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
                //session.insertNonAlignedRowRecords(paths, timestamps, values, types, null);
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
