package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.MetricsResource;
import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryResult;
import cn.edu.tsinghua.iginx.rest.insert.DataPointsParser;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestIT {
    private String insertJson = "[\n" +
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

    private static Session session;

    @BeforeClass
    public static void setUp() {
        session = new Session("127.0.0.1", 6888, "root", "root");
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error(e.getMessage());
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            session.closeSession();
        } catch (SessionException e) {
            LOGGER.error(e.getMessage());
        }
    }

    @Before
    public void insertData(){
        try{
            String json = insertJson;
            InputStream inputStream = new ByteArrayInputStream(json.getBytes());
            DataPointsParser parser = new DataPointsParser(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            parser.parse(false);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
        }
    }

    @After
    public void clearData() throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = session.executeSql(clearData);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            LOGGER.error("Clear date execute fail. Caused by: {}.", res.getParseErrorMsg());
            fail();
        }
    }

    public void executeAndCompare(String json,String output){
        String result = execute(json);
        assertEquals(result, output);
    }

    public String postQuery(String jsonStr) {
        try {
            if (jsonStr == null) {
                throw new Exception("query json must not be null or empty");
            }
            QueryParser parser = new QueryParser();
            Query query = parser.parseQueryMetric(jsonStr);
            QueryExecutor executor = new QueryExecutor(query);
            QueryResult result = executor.execute(false);
            String entity = parser.parseResultToJson(result, false);
            return entity;

        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return null;
        }
    }

    public String execute(String json){
        try {
            return postQuery(json);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return null;
        }
    }

    @Test
    public void testQueryWithoutTags(){
        String json ="{\n" +
                "\t\"start_absolute\" : 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t\t\"value\": \"5\",\n" +
                "\t\t\"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"time_zone\": \"Asia/Kabul\",\n" +
                "\t\"metrics\": [\n" +
                "\t\t{\n" +
                "\t\t\"name\": \"archive_file_tracked\"\n" +
                "\t\t},\n" +
                "\t\t{\n" +
                "\t\t\"name\": \"archive_file_search\"\n" +
                "\t\t}\n" +
                "\t]\n" +
                "}";
        String result = "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]},{\"sample_size\": 1,\"results\": [{ \"name\": \"archive_file_search\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"host\": [\"server2\"]}, \"values\": [[1359786400000,321.0]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryWithTags(){
        String json = "{\n" +
                "\t\"start_absolute\" : 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t\t\"value\": \"5\",\n" +
                "\t\t\"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"time_zone\": \"Asia/Kabul\",\n" +
                "\t\"metrics\": [\n" +
                "\t\t{\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "\t\t\"tags\": {\n" +
                "            \"host\": \"server1\"\n" +
                "        }\n" +
                "\t\t}\n" +
                "\t]\n" +
                "}";
        String result = "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryWrongTags(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "        \"tags\": {\n" +
                "            \"host\": [\"server2\"]\n" +
                "        }\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryOneTagWrong(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "        \"tags\": {\n" +
                "            \"host\": [\"server1\"],\n" +
                "            \"data_center\": [\"DC2\"]\n" +
                "        }\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryWrongName(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_\"\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive_\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryWrongTime(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_absolute\": 10,\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\"\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json,result);
    }

//    @Test
//    public void testQuery(){
//        String json = "";
//        String result = "";
//        executeAndCompare(json,result);
//    }


    @Test
    public void testQueryAvg(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "\t\t\"tags\": {\n" +
                "\t\t  \"host\": [\n" +
                "\t\t\t\"server1\"\n" +
                "\t\t  ]\n" +
                "\t\t},\n" +
                "\t\t\"aggregators\": [\n" +
                "\t\t  {\n" +
                "\t\t\t\"name\": \"avg\",\n" +
                "\t\t\t\"sampling\": {\n" +
                "\t\t\t  \"value\": 2,\n" +
                "\t\t\t  \"unit\": \"seconds\"\n" +
                "\t\t\t}\n" +
                "\t\t  }\n" +
                "\t\t]\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788298001,13.2],[1359788398001,123.3],[1359788408001,23.1]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryCount(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "\t\t\"aggregators\": [\n" +
                "\t\t  {\n" +
                "\t\t\t\"name\": \"count\",\n" +
                "\t\t\t\"sampling\": {\n" +
                "\t\t\t  \"value\": 1,\n" +
                "\t\t\t  \"unit\": \"days\"\n" +
                "\t\t\t}\n" +
                "\t\t  }\n" +
                "\t\t]\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,3]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryFirst(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "\t\t\"aggregators\": [\n" +
                "\t\t  {\n" +
                "\t\t\t\"name\": \"first\",\n" +
                "\t\t\t\"sampling\": {\n" +
                "\t\t\t  \"value\": 2,\n" +
                "\t\t\t  \"unit\": \"days\"\n" +
                "\t\t\t}\n" +
                "\t\t  }\n" +
                "\t\t]\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,13.2]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryLast(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "\t\t\"aggregators\": [\n" +
                "\t\t  {\n" +
                "\t\t\t\"name\": \"last\",\n" +
                "\t\t\t\"sampling\": {\n" +
                "\t\t\t  \"value\": 2,\n" +
                "\t\t\t  \"unit\": \"days\"\n" +
                "\t\t\t}\n" +
                "\t\t  }\n" +
                "\t\t]\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,23.1]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryMax(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "\t\t\"aggregators\": [\n" +
                "\t\t  {\n" +
                "\t\t\t\"name\": \"max\",\n" +
                "\t\t\t\"sampling\": {\n" +
                "\t\t\t  \"value\": 2,\n" +
                "\t\t\t  \"unit\": \"days\"\n" +
                "\t\t\t}\n" +
                "\t\t  }\n" +
                "\t\t]\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,123.3]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryMin(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "\t\t\"aggregators\": [\n" +
                "\t\t  {\n" +
                "\t\t\t\"name\": \"min\",\n" +
                "\t\t\t\"sampling\": {\n" +
                "\t\t\t  \"value\": 2,\n" +
                "\t\t\t  \"unit\": \"days\"\n" +
                "\t\t\t}\n" +
                "\t\t  }\n" +
                "\t\t]\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,13.2]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQuerySum(){
        String json = "{\n" +
                "\t\"start_absolute\": 1,\n" +
                "\t\"end_relative\": {\n" +
                "\t  \"value\": \"5\",\n" +
                "\t  \"unit\": \"days\"\n" +
                "\t},\n" +
                "\t\"metrics\": [\n" +
                "\t  {\n" +
                "\t\t\"name\": \"archive_file_tracked\",\n" +
                "\t\t\"aggregators\": [\n" +
                "\t\t  {\n" +
                "\t\t\t\"name\": \"sum\",\n" +
                "\t\t\t\"sampling\": {\n" +
                "\t\t\t  \"value\": 2,\n" +
                "\t\t\t  \"unit\": \"days\"\n" +
                "\t\t\t}\n" +
                "\t\t  }\n" +
                "\t\t]\n" +
                "\t  }\n" +
                "\t]\n" +
                "  }";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive_file_tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,159.6]]}]}]}";
        executeAndCompare(json,result);
    }

}
