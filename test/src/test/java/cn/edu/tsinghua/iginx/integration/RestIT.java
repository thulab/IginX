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
import java.io.*;
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
            "          \"value\": 321.0,\n" +
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
    public void insertData() {
        try{
            String json = insertJson;
            execute("insert.json",TYPE.INSERT);
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

    private enum TYPE {
       QUERY, INSERT, DELETE, DELETEMETRIC
    }

    public String orderGen(String json, TYPE type) {
        String ret = new String();
        if(type.equals(TYPE.DELETEMETRIC)) {
            ret = "curl -XDELETE";
            ret += " http://127.0.0.1:6666/api/v1/metric/{" + json + "}";
        } else {
            String prefix = "curl -XPOST -H\"Content-Type: application/json\" -d @";
            ret = prefix +  json;
            if(type.equals(TYPE.QUERY))
                ret += " http://127.0.0.1:6666/api/v1/datapoints/query";
            else if(type.equals(TYPE.INSERT)) ret += " http://127.0.0.1:6666/api/v1/datapoints";
            else if(type.equals(TYPE.DELETE)) ret += " http://127.0.0.1:6666/api/v1/datapoints/delete";
        }
        return ret;
    }

    public String execute(String json, TYPE type) throws Exception {
        String ret = new String();
        String curlArray = orderGen(json, type);
        Process process = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(curlArray.split(" "));
            processBuilder.directory(new File("./src/test/resources/restIT"));
            // 执行 url 命令
            process = processBuilder.start();

            // 输出子进程信息
            InputStreamReader inputStreamReaderINFO = new InputStreamReader(process.getInputStream());
            BufferedReader bufferedReaderINFO = new BufferedReader(inputStreamReaderINFO);
            String lineStr;
            while ((lineStr = bufferedReaderINFO.readLine()) != null) {
                ret += lineStr;
            }
            // 等待子进程结束
            process.waitFor();

            return ret;
        } catch (InterruptedException e) {
            // 强制关闭子进程（如果打开程序，需要额外关闭）
            process.destroyForcibly();
            return null;
        }
    }

    public void executeAndCompare(String json,String output){
        String result = new String();
        try {
            result = execute(json,TYPE.QUERY);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
        }
        assertEquals(output, result);
    }

    @Test
    public void testQueryWithoutTags(){
        String json ="testQueryWithoutTags.json";
        String result = "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]},{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.search\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"host\": [\"server2\"]}, \"values\": [[1359786400000,321.0]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryWithTags(){
        String json = "testQueryWithTags.json";
        String result = "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryWrongTags(){
        String json = "testQueryWrongTags.json";
        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryOneTagWrong(){
        String json = "testQueryOneTagWrong.json";
        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryWrongName(){
        String json = "testQueryWrongName.json";
        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive_\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryWrongTime(){
        String json = "testQueryWrongTime.json";
        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
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
        String json = "testQueryAvg.json";
        String result = "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788298001,13.2],[1359788398001,123.3],[1359788408001,23.1]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryCount(){
        String json = "testQueryCount.json";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,3]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryFirst(){
        String json = "testQueryFirst.json";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,13.2]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryLast(){
        String json = "testQueryLast.json";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,23.1]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryMax(){
        String json = "testQueryMax.json";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,123.3]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQueryMin(){
        String json = "testQueryMin.json";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,13.2]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testQuerySum(){
        String json = "testQuerySum.json";
        String result = "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,159.6]]}]}]}";
        executeAndCompare(json,result);
    }

    @Test
    public void testDelete()  throws Exception {
        String json = "testDelete.json";
        execute(json,TYPE.DELETE);

        String result = "{\"queries\":[{\"sample_size\": 2,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788410000,23.1]]}]}]}";
        json = "testQueryWithTags.json";
        executeAndCompare(json,result);
    }

    @Test
    public void testDeleteMetric()  throws Exception {
        String json = "archive.file.tracked";
        execute(json,TYPE.DELETEMETRIC);

        String result = "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        json = "testQueryWithTags.json";
        executeAndCompare(json,result);
    }
}
