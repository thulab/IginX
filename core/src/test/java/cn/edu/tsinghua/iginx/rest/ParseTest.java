package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.rest.bean.Metric;
import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryMetric;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class ParseTest {
    //测试数据更直观见注释
    /*
测试的query-json
{
	"start_absolute" : 486,
	"end_relative": {
		"value": "5",
		"unit": "days"
	},
	"metrics": [
		{
			"name": "rem.hero",
			"tags": {
			    "high": ["emi"],
				"name": ["lem", "blade"]
			},
			"aggregators": [
				{
					"name": "avg",
					"sampling": {
					"value": 2,
					"unit": "seconds"
					}
				},
				{
					"name": "dev",
					"sampling": {
					  "value": 2,
					  "unit": "seconds"
					},
					"return_type":"value"
				}
			]
		},
		{
			"name": "archive_file_search"
		}
	]
}
 */
    private String queryJson = "{\n" +
            "\t\"start_absolute\" : 486,\n" +
            "\t\"end_relative\": {\n" +
            "\t\t\"value\": \"5\",\n" +
            "\t\t\"unit\": \"days\"\n" +
            "\t},\n" +
            "\t\"metrics\": [\n" +
            "\t\t{\n" +
            "\t\t\t\"name\": \"rem.hero\",\n" +
            "\t\t\t\"tags\": {\n" +
            "\t\t\t    \"high\": [\"emi\"],\n" +
            "\t\t\t\t\"name\": [\"lem\", \"blade\"]\n" +
            "\t\t\t},\n" +
            "\t\t\t\"aggregators\": [\n" +
            "\t\t\t\t{\n" +
            "\t\t\t\t\t\"name\": \"avg\",\n" +
            "\t\t\t\t\t\"sampling\": {\n" +
            "\t\t\t\t\t\"value\": 2,\n" +
            "\t\t\t\t\t\"unit\": \"seconds\"\n" +
            "\t\t\t\t\t}\n" +
            "\t\t\t\t},\n" +
            "\t\t\t\t{\n" +
            "\t\t\t\t\t\"name\": \"dev\",\n" +
            "\t\t\t\t\t\"sampling\": {\n" +
            "\t\t\t\t\t  \"value\": 2,\n" +
            "\t\t\t\t\t  \"unit\": \"weeks\"\n" +
            "\t\t\t\t\t},\n" +
            "\t\t\t\t\t\"return_type\":\"value\"\n" +
            "\t\t\t\t}\n" +
            "\t\t\t]\n" +
            "\t\t},\n" +
            "\t\t{\n" +
            "\t\t\t\"name\": \"archive_file_search\"\n" +
            "\t\t}\n" +
            "\t]\n" +
            "}";

    String insertJson = "[\n" +
            "    {\n" +
            "        \"name\": \"archive_file_tracked\",\n" +
            "        \"datapoints\": [\n" +
            "            [1359788400000, 123.3],\n" +
            "            [1359788300000, 13.2 ],\n" +
            "            [1359788410000, 23.1 ]\n" +
            "        ],\n" +
            "        \"tags\": {\n" +
            "            \"host\": \"server1\",\n" +
            "            \"data_center\": \"DC1\"\n" +
            "        },\n" +
            "        \"annotation\": {\n" +
            "        \"category\": [\"cat1\"],\n" +
            "        \"title\": \"text\",\n" +
            "        \"description\": \"desp\"\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "          \"name\": \"archive_file_search\",\n" +
            "          \"timestamp\": 1359786400000,\n" +
            "          \"value\": 321,\n" +
            "          \"tags\": {\n" +
            "              \"host\": \"server2\"\n" +
            "          }\n" +
            "      }\n" +
            "]";
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private List<Metric> metricList = new ArrayList<>();

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

//    @Test
//    public void testParseQuery() {
//        try{
//            String json = queryJson;
//            QueryParser parser = new QueryParser();
//            Query query = parser.parseQueryMetric(json);
//            for (QueryMetric queryMetric : query.getQueryMetrics()) {
//                testParseQueryTime(query);
//                testParseQueryMetrics(queryMetric);
//            }
//        } catch (Exception e) {
//            LOGGER.error("Error occurred during execution ", e);
//        }
//
//    }

    private Metric getMetricObject(JsonNode node, boolean isAnnotation) {
        Metric ret = new Metric();
        ret.setName(node.get("name").asText());
        Iterator<String> fieldNames = node.get("tags").fieldNames();
        Iterator<JsonNode> elements = node.get("tags").elements();
        //insert语句的tag只能有一个val，是否有问题？
        while (elements.hasNext() && fieldNames.hasNext()) {
            ret.addTag(fieldNames.next(), elements.next().textValue());
        }
        JsonNode tim = node.get("timestamp"), val = node.get("value");
        if (tim != null && val != null) {
            ret.addKey(tim.asLong());
            ret.addValue(val.asText());
        }
        JsonNode dp = node.get("datapoints");
        if (dp != null) {
            if (dp.isArray()) {
                for (JsonNode dpnode : dp) {
                    if (isAnnotation) {
                        ret.addKey(dpnode.asLong());
                    } else if (dpnode.isArray()) {
                        ret.addKey(dpnode.get(0).asLong());
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

    public List<Metric> parse(boolean isAnnotation,String json) throws Exception {
        InputStream inputStream = new ByteArrayInputStream(json.getBytes());
        JsonNode node = mapper.readTree(inputStream);
        if (node.isArray()) {
            for (JsonNode objNode : node) {
                metricList.add(getMetricObject(objNode, isAnnotation));
            }
        } else {
            metricList.add(getMetricObject(node, isAnnotation));
        }
        return metricList;
    }

    @Test
    public void testParseInsertMetricsTags() {
        try {
            String json = insertJson;
            List<Metric> metricLists = parse(false,json);
            Map<String, String> tagsList = new HashMap<>();

            tagsList = metricLists.get(0).getTags();
            Iterator<Map.Entry<String, String>> entries = tagsList.entrySet().iterator();
            Map.Entry<String, String> it = entries.next();
            assertEquals(it.getKey(), "data_center");
            assertEquals(it.getValue(), "DC1");
            it = entries.next();
            assertEquals(it.getKey(), "host");
            assertEquals(it.getValue(), "server1");

            tagsList = metricLists.get(1).getTags();
            entries = tagsList.entrySet().iterator();
            it = entries.next();
            assertEquals(it.getKey(), "host");
            assertEquals(it.getValue(), "server2");
        } catch(Exception e){
            LOGGER.error("Error occurred during execution ", e);
        }
    }

    @Test
    public void testParseInsertMetricsName() {
        try {
            int pos = 0;
            String json = insertJson;
            List<Metric> metricLists = parse(false,json);
            for (Metric metric : metricLists) {
                if(pos == 0){
                    assertEquals(metric.getName(), "archive_file_tracked");
                }else{
                    assertEquals(metric.getName(), "archive_file_search");
                }
                pos++;
            }
        } catch(Exception e){
            LOGGER.error("Error occurred during execution ", e);
        }
    }

    @Test
    public void testParseInsertMetricsTimes() {
        try {
            int pos = 0;
            String json = insertJson;
            List<Metric> metricLists = parse(false,json);
            for (Metric metric : metricLists) {
                if(pos == 0){
                    Long time0 = 1359788400000L;
                    Long time1 = 1359788300000L;
                    Long time2 = 1359788410000L;
                    assertEquals(metric.getKeys().get(0), time0);
                    assertEquals(metric.getKeys().get(1), time1);
                    assertEquals(metric.getKeys().get(2), time2);
                }else{
                    Long time0 = 1359786400000L;
                    assertEquals(metric.getKeys().get(0), time0);
                }
                pos++;
            }
        } catch(Exception e){
            LOGGER.error("Error occurred during execution ", e);
        }
    }

    @Test
    public void testParseInsertMetricsValues() {
        try {
            int pos = 0;
            String json = insertJson;
            List<Metric> metricLists = parse(false,json);
            for (Metric metric : metricLists) {
                if (pos == 0) {
                    assertEquals(metric.getValues().get(0), "123.3");
                    assertEquals(metric.getValues().get(1), "13.2");
                    assertEquals(metric.getValues().get(2), "23.1");
                } else {
                    assertEquals(metric.getValues().get(0), "321");
                }
                pos++;
            }
        } catch(Exception e){
            LOGGER.error("Error occurred during execution ", e);
        }
    }

//    @Test
//    public void testParseQueryTime() {
//        try{
//            String json = queryJson;
//            QueryParser parser = new QueryParser();
//            Query query = parser.parseQueryMetric(json);
//
//            long now = System.currentTimeMillis();
//            long v = 5L;
//            Long time = transTimeFromString("days");
//            Long endRelative = now - v * time;
//            Long startRelative = 486L;
//
//            assertEquals(endRelative, query.getEndAbsolute());
//            assertEquals(startRelative, query.getStartAbsolute());
//        } catch (Exception e) {
//            LOGGER.error("Error occurred during execution ", e);
//        }
//    }

    @Test
    public void testParseQueryMetricsTags() {
        try{
            String json = queryJson;
            QueryParser parser = new QueryParser();
            Query query = parser.parseQueryMetric(json);
            int MetricNum = 0;

            for (QueryMetric queryMetric : query.getQueryMetrics()) {
                for (Map.Entry<String, List<String>> entry : queryMetric.getTags().entrySet()) {
                    String mapKey = entry.getKey();
                    if(MetricNum==0){
                        assertEquals(mapKey, "high");
                        assertEquals(entry.getValue().get(0), "emi");
                    }
                    else {
                        assertEquals(mapKey, "name");
                        assertEquals(entry.getValue().get(0), "lem");
                        assertEquals(entry.getValue().get(1), "blade");
                    }
                    MetricNum++;
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
        }
    }

    @Test
    public void testParseQueryMetricsName() {
        try{
            String json = queryJson;
            QueryParser parser = new QueryParser();
            Query query = parser.parseQueryMetric(json);

            assertEquals(query.getQueryMetrics().get(0).getName(), "rem.hero");
            assertEquals(query.getQueryMetrics().get(1).getName(), "archive_file_search");
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
        }
    }

    @Test
    public void testParseQueryAggregators() {
        try{
            String json = queryJson;
            QueryParser parser = new QueryParser();
            Query query = parser.parseQueryMetric(json);

            QueryMetric queryMetric = query.getQueryMetrics().get(0);
            assertEquals(QueryAggregatorType.AVG,queryMetric.getAggregators().get(0).getType());
            Long dur = 2*transTimeFromString("seconds");
            assertEquals(dur,queryMetric.getAggregators().get(0).getDur());

            assertEquals(QueryAggregatorType.DEV,queryMetric.getAggregators().get(1).getType());
            dur = 2*transTimeFromString("weeks");
            assertEquals(dur,queryMetric.getAggregators().get(1).getDur());
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
        }
    }
}