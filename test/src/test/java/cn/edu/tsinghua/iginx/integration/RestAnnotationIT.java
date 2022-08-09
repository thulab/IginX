package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.MetricsResource;
import cn.edu.tsinghua.iginx.rest.bean.Query;
import cn.edu.tsinghua.iginx.rest.bean.QueryMetric;
import cn.edu.tsinghua.iginx.rest.bean.QueryResult;
import cn.edu.tsinghua.iginx.rest.bean.QueryResultDataset;
import cn.edu.tsinghua.iginx.rest.insert.DataPointsParser;
import cn.edu.tsinghua.iginx.rest.insert.InsertWorker;
import cn.edu.tsinghua.iginx.rest.query.QueryExecutor;
import cn.edu.tsinghua.iginx.rest.query.QueryParser;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static cn.edu.tsinghua.iginx.rest.bean.SpecialTime.MAXTIEM;
import static cn.edu.tsinghua.iginx.rest.bean.SpecialTime.TOPTIEM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RestAnnotationIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);
    private static final String ERROR = "ERROR";
    private String insertJson = "[{\n" +
            "        \"name\": \"archive_file_tracked.ann\",\n" +
            "        \"datapoints\": [\n" +
            "                [1359788400000, 123.3],\n" +
            "                [1359788300000, 13.2],\n" +
            "                [1359788410000, 23.1]\n" +
            "        ],\n" +
            "        \"tags\": {\n" +
            "                \"host\": \"server1\",\n" +
            "                \"data_center\": \"DC1\"\n" +
            "        },\n" +
            "        \"annotation\": {\n" +
            "                \"category\": [\"cat3\"],\n" +
            "                \"title\" : \"title1\",\n" +
            "                \"description\" : \"dsp1\"\n" +
            "        }\n" +
            "},{\n" +
            "        \"name\": \"archive_file_tracked.ann\",\n" +
            "        \"datapoints\": [\n" +
            "                [1359788400000, 123.3],\n" +
            "                [1359788300000, 13.2],\n" +
            "                [1359788410000, 23.1]\n" +
            "        ],\n" +
            "        \"tags\": {\n" +
            "                \"host\": \"server2\",\n" +
            "                \"data_center\": \"DC2\"\n" +
            "        },\n" +
            "        \"annotation\": {\n" +
            "                \"category\": [\"cat3\"],\n" +
            "                \"title\" : \"title12\",\n" +
            "                \"description\" : \"dsp12\"\n" +
            "        }\n" +
            "      },{\n" +
            "        \"name\": \"archive_file_tracked.bcc\",\n" +
            "        \"datapoints\": [\n" +
            "                [1359788400000, 123.3],\n" +
            "                [1359788300000, 13.2],\n" +
            "                [1359788410000, 23.1]\n" +
            "        ],\n" +
            "        \"tags\": {\n" +
            "                \"host\": \"server1\",\n" +
            "                \"data_center\": \"DC1\"\n" +
            "        },\n" +
            "        \"annotation\": {\n" +
            "                \"category\": [\"cat2\"],\n" +
            "                \"title\" : \"titlebcc\",\n" +
            "                \"description\" : \"dspbcc\"\n" +
            "        }\n" +
            "}]";

    String appendJson = "{\n" +
            "    \"start_absolute\": 1359788400000,\n" +
            "    \"end_absolute\" : 1359788400001,\n" +
            "    \"metrics\": [{\n" +
            "            \"name\": \"archive_file_tracked.ann\",\n" +
            "            \"tags\": {\n" +
            "                    \"host\": [\"server1\"],\n" +
            "                    \"data_center\": [\"DC1\"]\n" +
            "            },\n" +
            "            \"annotation\": {\n" +
            "                    \"category\": [\"cat3\",\"cat4\"],\n" +
            "                    \"title\" : \"titleNewUp\",\n" +
            "                    \"description\" : \"dspNewUp\"\n" +
            "            }\n" +
            "    },{\n" +
            "        \"name\": \"archive_file_tracked.bcc\",\n" +
            "        \"tags\": {\n" +
            "                \"host\": [\"server1\"],\n" +
            "                \"data_center\": [\"DC1\"]\n" +
            "        },\n" +
            "        \"annotation\": {\n" +
            "                \"category\": [\"cat3\",\"cat4\"],\n" +
            "                \"title\" : \"titleNewUpbcc\",\n" +
            "                \"description\" : \"dspNewUpbcc\"\n" +
            "        }\n" +
            "}]\n" +
            "}";

    String updateJson = "{\n" +
            "    \"metrics\": [{\n" +
            "            \"name\": \"archive_file_tracked.ann\",\n" +
            "            \"tags\": {\n" +
            "                    \"host\": [\"server1\"],\n" +
            "                    \"data_center\": [\"DC1\"]\n" +
            "            },\n" +
            "            \"annotation\": {\n" +
            "                \"category\": [\"cat3\"]\n" +
            "            },\n" +
            "            \"annotation-new\": {\n" +
            "                \"category\": [\"cat6\"],\n" +
            "                \"title\" : \"titleNewUp111\",\n" +
            "                \"description\" : \"dspNewUp111\"\n" +
            "            }\n" +
            "    },{\n" +
            "        \"name\": \"archive_file_tracked.bcc\",\n" +
            "        \"tags\": {\n" +
            "                \"host\": [\"server1\"],\n" +
            "                \"data_center\": [\"DC1\"]\n" +
            "        },\n" +
            "        \"annotation\": {\n" +
            "            \"category\": [\"cat2\"]\n" +
            "        },\n" +
            "        \"annotation-new\": {\n" +
            "            \"category\": [\"cat6\"],\n" +
            "            \"title\" : \"titleNewUp111bcc\",\n" +
            "            \"description\" : \"dspNewUp111bcc\"\n" +
            "        }\n" +
            "}]\n" +
            "}\n";

    private static Session session;
    private enum TYPE
    {
        APPEND,UPDATE,INSERT,QUERYANNO,QUERYALL,DELETE
    }

    @BeforeClass
    public static void setUp() {
        session = new Session("127.0.0.1", 6888, "root", "root");
        try {
            session.openSession();
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

    @AfterClass
    public static void tearDown() {
        try {
            session.closeSession();
        } catch (SessionException e) {
            LOGGER.error(e.getMessage());
        }
    }


    private Query getAnnoDataQueryFromTimeSeries(Query query, QueryResult result){
        Query ret = new Query();
        QueryParser parser = new QueryParser();
        //筛选出符合全部anno信息的路径
        for(int i=0;i<query.getQueryMetrics().size();i++){
            List<String> paths = new ArrayList<>();
            for(int j=0; j<result.getQueryResultDatasets().size(); j++){
                QueryResultDataset data = result.getQueryResultDatasets().get(j);
                paths = parser.getPrefixPaths(query.getQueryMetrics().get(i).getAnnotationLimit().getTag(),data.getPaths());
            }
            for(String pathStr : paths){
                QueryMetric metric = new QueryMetric();
                metric = parser.parseQueryResultAnnoDataPaths(pathStr);
                metric.setAnnotationLimit(query.getQueryMetrics().get(i).getAnnotationLimit());
                ret.addQueryMetrics(metric);
            }
        }
        return ret;
    }

    private QueryResult getAnno(Query queryAnno) throws Exception  {
        //查找title以及description信息
        queryAnno.setStartAbsolute(1L);//LHZ这里可以不用查出所有数据，可以优化
        queryAnno.setEndAbsolute(MAXTIEM);
        QueryExecutor executorAnno = new QueryExecutor(queryAnno);//要确认下是否annotation信息在查找时会影响结果
        QueryResult resultAnno = executorAnno.execute(false);

        try {
            executorAnno.queryAnno(resultAnno);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            throw e;
        }
        return resultAnno;
    }

    public QueryResult getAnnoDataQueryFromTitle(Query query, QueryResult result) {
        QueryResult ret = new QueryResult();
        int len = result.getQueryResultDatasets().size();
        for(int i=0; i<len; i++) {
            QueryResultDataset dataRet = new QueryResultDataset();
            QueryResultDataset data = result.getQueryResultDatasets().get(i);
            String title = result.getQueryMetrics().get(i).getAnnotationLimit().getTitle();
            for(int j=0; j<data.getPaths().size(); j++) {
                if(title.equals(".*") || title.isEmpty() || (data.getTitles().get(j)!=null && data.getTitles().get(j).equals(title)) ) {
                    dataRet.addPlus(data.getPaths().get(j),data.getDescriptions().get(j),data.getCategorys().get(j),
                            data.getValueLists().get(j),data.getTimeLists().get(j));
                }
            }
            ret.addqueryResultDataset(dataRet);
            ret.addQueryMetric(result.getQueryMetrics().get(i));
        }
        return ret;
    }

    public String postQuery(String jsonStr, boolean isAnnotation, boolean isAnnoData, boolean isGrafana) {
        try {
            if (jsonStr == null) {
                throw new Exception("query json must not be null or empty");
            }
            QueryParser parser = new QueryParser();
            Query query = isAnnotation ? parser.parseAnnotationQueryMetric(jsonStr, isGrafana) : parser.parseQueryMetric(jsonStr);
            if (!isAnnotation) {
                QueryExecutor executor = new QueryExecutor(query);
                QueryResult result = executor.execute(false);
                String entity = parser.parseResultToJson(result, false);
                return entity;
            } else if (isAnnoData) {
                QueryExecutor executor = new QueryExecutor(null);
                //调用show time series
                QueryResult timeSeries = executor.executeShowTimeSeries();
                //筛选路径信息，正常信息路径，生成单个query
                Query queryAnnoData = getAnnoDataQueryFromTimeSeries(query, timeSeries);
                //先查询title信息
                //查询anno的title以及dsp信息
                QueryResult resultAnno = getAnno(queryAnnoData);

                //筛选出符合title信息的序列
                QueryResult result = getAnnoDataQueryFromTitle(query, resultAnno);
//                queryAnnoData.setStartAbsolute(1L);
//                queryAnnoData.setEndAbsolute(TOPTIEM);
//                executor = new QueryExecutor(queryAnnoData);
//                QueryResult resultData = executor.execute(false);

                //这里的解析也要确认是否可以解析成功？？LHZ
                parser.getAnnoCategory(resultAnno);
                String entity = parser.parseAnnoDataResultToJson(result);
                return entity;
            } else {//只查询anno信息
                //查找出所有符合tagkv的序列路径
                Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);
                Query queryAnno = new Query();
                queryAnno.setQueryMetrics(queryBase.getQueryMetrics());

                //查询anno的title以及dsp信息
                QueryResult result = getAnno(queryAnno);

                //这里的解析也要确认是否可以解析成功？？LHZ
                parser.getAnnoCategory(result);
                String entity = parser.parseAnnoResultToJson(result);
                return entity;
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return ERROR;
        }
    }

    public void executeAndCompare(String json, String output, TYPE type) {
        String result = executeQuery(json,type);
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
            //LHZBUG这里要进行相应更新
//            String entity = isAnnotation ? parser.parseResultToAnnotationJson(result, isGrafana) : parser.parseResultToJson(result, false);
            String entity = parser.parseResultToJson(result, false);
            return entity;

        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return null;
        }
    }

    //查询调用接口
    public String executeQuery(String json, TYPE type){
        try {
            switch (type) {
                case QUERYANNO:
                    return postQuery(json,true,false,false);
                case QUERYALL:
                    return postQuery(json,true,true,false);
                default:
                    return null;
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            return null;
        }
    }

    public void appendAnno(String jsonStr) throws Exception {
        //查找出所有符合tagkv的序列路径
        QueryParser parser = new QueryParser();
        Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);
        QueryExecutor executorPath = new QueryExecutor(queryBase);
        QueryResult result = executorPath.execute(false);

        //修改路径，得到所有的单条路径信息，便于之后操作
        Query queryAll = parser.splitPath(result, queryBase);
        queryAll.setStartAbsolute(queryBase.getStartAbsolute());
        queryAll.setEndAbsolute(queryBase.getEndAbsolute());

        //执行删除操作,还是要先执行删除，因为之后执行删除，可能会删除已经插入的序列值
        //LHZ 确保删除是可以正确实现的，同时要确认是否可以通过@路径确切删除
        //LHZ 还是给出tag，是删除包含，还是完全匹配的tag删除
        QueryExecutor executorData = new QueryExecutor(queryAll);
        executorData.execute(true);

        //修改路径，并重新查询数据，并插入数据
        run(true, queryBase, result);
    }

    public void updateAnno(String jsonStr) throws Exception {
        //查找出所有符合tagkv的序列路径
        QueryParser parser = new QueryParser();
        Query queryBase = parser.parseAnnotationQueryMetric(jsonStr, false);
        //加入category路径信息
        Query querySp = parser.addAnnoTags(queryBase);
        //添加last聚合查询
//        queryBase.addLastAggregator();
        querySp.setStartAbsolute(1L);
        querySp.setEndAbsolute(TOPTIEM);
        QueryExecutor executorPath = new QueryExecutor(querySp);//LHZ要确认下是否annotation信息在查找时会影响结果
        QueryResult resultALL = executorPath.execute(false);



        //修改路径，并重新查询数据，并插入数据
        run(false, querySp, resultALL);

        //找到精确路径
        Query queryAll = parser.getSpecificQuery(resultALL, queryBase);
        queryAll.setStartAbsolute(1L);
        queryAll.setEndAbsolute(TOPTIEM);
        QueryExecutor executorData = new QueryExecutor(queryAll);
        //执行删除操作
        executorData.deleteMetric();
    }

    public void run(boolean isAppend, Query preQuery, QueryResult preQueryResult) {
        Response response;
        try {
            if(isAppend) {
                DataPointsParser parser = new DataPointsParser();
                parser.handleAnnotationAppend(preQuery, preQueryResult);
            } else {
                DataPointsParser parser = new DataPointsParser();
                parser.handleAnnotationUpdate(preQuery, preQueryResult);
            }
            response = Response.status(Response.Status.OK).build();
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
        }
    }

    @Test
    public void testQueryAnno() {
        String query = "{\n" +
                "    \"metrics\": [{\n" +
                "        \"name\": \"archive_file_tracked.ann\",\n" +
                "        \"tags\": {\n" +
                "            \"host\": [\"server1\"],\n" +
                "            \"data_center\": [\"DC1\"]\n" +
                "        }\n" +
                "    }, {\n" +
                "        \"name\": \"archive_file_tracked.bcc\",\n" +
                "        \"tags\": {\n" +
                "            \"host\": [\"server1\"],\n" +
                "            \"data_center\": [\"DC1\"]\n" +
                "        }\n" +
                "    }]\n" +
                "}\n";
        String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titlebcc\",\"description\": \"dspbcc\",\"category\": [\"cat2\"]}}]}";
        executeAndCompare(query, ans, TYPE.QUERYANNO);
    }

    @Test
    public void testQueryAll() {
        String query = "{\n" +
                "    \"metrics\": [{\n" +
                "            \"annotation\": {\n" +
                "                    \"category\": [\"cat2\"]\n" +
                "            }\n" +
                "    }]\n" +
                "}";
        String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titlebcc\",\"description\": \"dspbcc\",\"category\": [\"cat2\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]}";
        executeAndCompare(query, ans, TYPE.QUERYALL);
    }

    @Test
    public void testAppend() {
        try {
            String json = appendJson;
            appendAnno(json);
            String query = "{\n" +
                    "    \"metrics\": [{\n" +
                    "        \"name\": \"archive_file_tracked.ann\",\n" +
                    "        \"tags\": {\n" +
                    "            \"host\": [\"server1\"],\n" +
                    "            \"data_center\": [\"DC1\"]\n" +
                    "        }\n" +
                    "    }, {\n" +
                    "        \"name\": \"archive_file_tracked.bcc\",\n" +
                    "        \"tags\": {\n" +
                    "            \"host\": [\"server1\"],\n" +
                    "            \"data_center\": [\"DC1\"]\n" +
                    "        }\n" +
                    "    }]\n" +
                    "}\n";
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp\",\"description\": \"dspNewUp\",\"category\": [\"cat3\",\"cat4\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titlebcc\",\"description\": \"dspbcc\",\"category\": [\"cat2\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUpbcc\",\"description\": \"dspNewUpbcc\",\"category\": [\"cat2\",\"cat3\",\"cat4\"]}}]}";
            executeAndCompare(query, ans, TYPE.QUERYANNO);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
        }
    }

    @Test
    public void testUpdate() {
        try {
            String json = updateJson;
            updateAnno(json);
            String query = "{\n" +
                    "    \"metrics\": [{\n" +
                    "            \"annotation\": {\n" +
                    "                    \"category\": [\"cat6\"]\n" +
                    "            }\n" +
                    "    }]\n" +
                    "}";
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp111bcc\",\"description\": \"dspNewUp111bcc\",\"category\": [\"cat6\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp111\",\"description\": \"dspNewUp111\",\"category\": [\"cat6\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]}";
            executeAndCompare(query, ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
        }
    }


}
