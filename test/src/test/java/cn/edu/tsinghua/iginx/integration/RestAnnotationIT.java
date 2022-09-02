package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.MetricsResource;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/*
一、anntation测试逻辑：
1、正确操作测试，验证单一操作正确性
2、错误操作测试，验证错误操作，或者无效操作结果
3、重复性操作测试，测试可重复操作的结果是否正确
4、操作对象重复，测试操作逻辑中，可重复添加的元素是否符合逻辑
5、复杂操作，测试多种操作组合结果是否正确

二、anntation测试条目：
1、查询anntation信息
2、查询数据以及annotation信息
3、对每个修改操作单独测试，并通过两种查询分别验证正确性：
    3.1、测试 add（增加标签操作），通过queryAnno以及queryAll两种方法测试
    3.2、测试 update（更新标签操作），通过queryAnno以及queryAll两种方法测试
    3.3、测试 delete（删除标签操作），通过queryAnno以及queryAll两种方法测试
4、测试重复性操作操作，查看结果正确性
    4.1、测试添加相同category，通过queryAnno以及queryAll两种方法测试
    4.2、测试不断更新相同结果的category，通过queryAnno以及queryAll两种方法测试
    4.3、测试不断删除的category，通过queryAnno以及queryAll两种方法测试
5、逻辑上重复的操作，如更新结果与原category相同，查看结果正确性
6、复杂操作，插入，添加，更新，删除，每步操作查看结果正确性

 */
public class RestAnnotationIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsResource.class);

    private static Session session;

    private enum TYPE {
        APPEND, UPDATE, INSERT, QUERYANNO, QUERYALL, DELETE
    }

    private String API[] = {
            " http://127.0.0.1:6666/api/v1/datapoints/annotations/add",
            " http://127.0.0.1:6666/api/v1/datapoints/annotations/update",
            " http://127.0.0.1:6666/api/v1/datapoints/annotations",
            " http://127.0.0.1:6666/api/v1/datapoints/query/annotations",
            " http://127.0.0.1:6666/api/v1/datapoints/query/annotations/data",
            " http://127.0.0.1:6666/api/v1/datapoints/annotations/delete",
    };

    public String orderGen(String fileName, TYPE type) {
        String ret = new String();
        String prefix = "curl -XPOST -H\"Content-Type: application/json\" -d @";
        ret = prefix + fileName;
        ret += API[type.ordinal()];
        return ret;
    }

    public String execute(String fileName, TYPE type) throws Exception {
        String ret = new String();
        String curlArray = orderGen(fileName, type);
        Process process = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(curlArray.split(" "));
            processBuilder.directory(new File("./src/test/resources/restAnnotation"));
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


    @BeforeClass
    public static void setUp() {
        session = new Session("127.0.0.1", 6888, "root", "root");
        try {
            session.openSession();
        } catch (SessionException e) {
            LOGGER.error(e.getMessage());
            fail();
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            session.closeSession();
        } catch (SessionException e) {
            LOGGER.error(e.getMessage());
            fail();
        }
    }

    @Before
    public void insertData() {
        try {
            execute("insert.json", TYPE.INSERT);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
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

    public void executeAndCompare(String json, String output, TYPE type) {
        try {
            String result = execute(json, type);
            assertEquals(output, result);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testQueryAnno() {
        String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titlebcc\",\"description\": \"dspbcc\",\"category\": [\"cat2\"]}}]}";
        executeAndCompare("queryAnno.json", ans, TYPE.QUERYANNO);
    }

    @Test
    public void testQueryAll() {
        String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"title12\",\"description\": \"dsp12\",\"category\": [\"cat3\",\"cat4\"]}, \"values\": [[1359788300000,55.0],[1359788400000,44.0],[1359788410000,66.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788400000,11.0],[1359788410000,33.0]]}]}";
        executeAndCompare("queryData.json", ans, TYPE.QUERYALL);
    }

    @Test
    public void testAppendViaQueryAnno() {
        try {
            execute("add.json", TYPE.APPEND);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp\",\"description\": \"dspNewUp\",\"category\": [\"cat3\",\"cat4\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titlebcc\",\"description\": \"dspbcc\",\"category\": [\"cat2\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUpbcc\",\"description\": \"dspNewUpbcc\",\"category\": [\"cat2\",\"cat3\",\"cat4\"]}}]}";
            executeAndCompare("queryAppendViaQueryAnno.json", ans, TYPE.QUERYANNO);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testAppendViaQueryAll() {
        try {
            execute("add.json", TYPE.APPEND);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp\",\"description\": \"dspNewUp\",\"category\": [\"cat3\",\"cat4\"]}, \"values\": [[1359788400000,11.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"title12\",\"description\": \"dsp12\",\"category\": [\"cat3\",\"cat4\"]}, \"values\": [[1359788300000,55.0],[1359788400000,44.0],[1359788410000,66.0]]},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUpbcc\",\"description\": \"dspNewUpbcc\",\"category\": [\"cat2\",\"cat3\",\"cat4\"]}, \"values\": [[1359788400000,77.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788410000,33.0]]}]}";
            executeAndCompare("queryAppendViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testUpdateViaQueryAll() {
        try {
            execute("update.json", TYPE.UPDATE);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"titleNewUp111\",\"description\": \"dspNewUp111\",\"category\": [\"cat6\"]}, \"values\": [[1359788300000,55.0],[1359788400000,44.0],[1359788410000,66.0]]},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp111bcc\",\"description\": \"dspNewUp111bcc\",\"category\": [\"cat6\"]}, \"values\": [[1359788300000,88.0],[1359788400000,77.0],[1359788410000,99.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788400000,11.0],[1359788410000,33.0]]}]}";
            executeAndCompare("queryUpdateViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testUpdateViaQueryAnno() {
        try {
            execute("update.json", TYPE.UPDATE);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"titleNewUp111\",\"description\": \"dspNewUp111\",\"category\": [\"cat6\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp111bcc\",\"description\": \"dspNewUp111bcc\",\"category\": [\"cat6\"]}}]}";
            executeAndCompare("queryUpdateViaQueryAnno.json", ans, TYPE.QUERYANNO);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testDeleteViaQueryAll() {
        try {
            execute("delete.json", TYPE.DELETE);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788400000,11.0],[1359788410000,33.0]]}]}";
            executeAndCompare("deleteViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testDeleteViaQueryAnno() {
        try {
            execute("delete.json", TYPE.DELETE);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titlebcc\",\"description\": \"dspbcc\",\"category\": [\"cat2\"]}}]}";
            executeAndCompare("deleteViaQueryAnno.json", ans, TYPE.QUERYANNO);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    /*
    4、测试重复性操作操作，查看结果正确性
    4.1、测试添加相同category，通过queryAnno以及queryAll两种方法测试
    4.2、测试不断更新相同结果的category，通过queryAnno以及queryAll两种方法测试
     */

    @Test
    public void testDuplicateAppendViaAueryAnno() {
        try {
            String clearData = "CLEAR DATA;";
            session.executeSql(clearData);

            execute("insert2.json", TYPE.INSERT);
            execute("add2.json", TYPE.APPEND);
            execute("add2.json", TYPE.APPEND);

            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp\",\"description\": \"dspNewUp\",\"category\": [\"zat3\",\"zat4\"]}, \"values\": [[1359788300001,55.0],[1359788400000,11.0],[1359788400001,44.0],[1359788410001,66.0]]},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUpbcc\",\"description\": \"dspNewUpbcc\",\"category\": [\"cat2\",\"cat3\",\"cat4\"]}, \"values\": [[1359788400000,77.0]]}]}";
            executeAndCompare("testAppend2ViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testDuplicateAppendViaQueryAll() {
        try {
            execute("add.json", TYPE.APPEND);
            execute("add.json", TYPE.APPEND);

            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp\",\"description\": \"dspNewUp\",\"category\": [\"cat3\",\"cat4\"]}, \"values\": [[1359788400000,11.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"title12\",\"description\": \"dsp12\",\"category\": [\"cat3\",\"cat4\"]}, \"values\": [[1359788300000,55.0],[1359788400000,44.0],[1359788410000,66.0]]},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUpbcc\",\"description\": \"dspNewUpbcc\",\"category\": [\"cat2\",\"cat3\",\"cat4\"]}, \"values\": [[1359788400000,77.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788410000,33.0]]}]}";
            executeAndCompare("queryAppendViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testDuplicateUpdateViaAueryAll() {
        try {
            execute("update.json", TYPE.UPDATE);
            execute("update.json", TYPE.UPDATE);

            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"titleNewUp111\",\"description\": \"dspNewUp111\",\"category\": [\"cat6\"]}, \"values\": [[1359788300000,55.0],[1359788400000,44.0],[1359788410000,66.0]]},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp111bcc\",\"description\": \"dspNewUp111bcc\",\"category\": [\"cat6\"]}, \"values\": [[1359788300000,88.0],[1359788400000,77.0],[1359788410000,99.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788400000,11.0],[1359788410000,33.0]]}]}";
            executeAndCompare("queryUpdateViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testDuplicateUpdateViaQueryAnno() {
        try {
            execute("update.json", TYPE.UPDATE);
            execute("update.json", TYPE.UPDATE);

            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"titleNewUp111\",\"description\": \"dspNewUp111\",\"category\": [\"cat6\"]}},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp111bcc\",\"description\": \"dspNewUp111bcc\",\"category\": [\"cat6\"]}}]}";
            executeAndCompare("queryUpdateViaQueryAnno.json", ans, TYPE.QUERYANNO);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testDuplicateDeleteViaQueryAll() {
        try {
            execute("delete.json", TYPE.DELETE);
            execute("delete.json", TYPE.DELETE);

            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788400000,11.0],[1359788410000,33.0]]}]}";
            executeAndCompare("deleteViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    /*
    5、逻辑上重复的操作，如更新结果与原category相同，查看结果正确性
     */

    @Test
    public void testSameUpdateViaQueryAll() {
        try {
            execute("updateSame.json", TYPE.UPDATE);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"titleNewUp111\",\"description\": \"dspNewUp111\",\"category\": [\"cat3\",\"cat4\"]}, \"values\": [[1359788300000,55.0],[1359788400000,44.0],[1359788410000,66.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"title1\",\"description\": \"dsp1\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788400000,11.0],[1359788410000,33.0]]}]}";
            executeAndCompare("queryData.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testSameAppendViaQueryAll() {
        try {
            execute("addSame.json", TYPE.APPEND);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC2\"],\"host\" : [\"server2\"]},\"annotation\": {\"title\": \"title12\",\"description\": \"dsp12\",\"category\": [\"cat3\",\"cat4\"]}, \"values\": [[1359788300000,55.0],[1359788400000,44.0],[1359788410000,66.0]]},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUpbcc\",\"description\": \"dspNewUpbcc\",\"category\": [\"cat2\",\"cat3\",\"cat4\"]}, \"values\": [[1359788400000,77.0]]},{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp\",\"description\": \"dspNewUp\",\"category\": [\"cat3\"]}, \"values\": [[1359788300000,22.0],[1359788400000,11.0],[1359788410000,33.0]]}]}";
            executeAndCompare("queryAppendViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    /*
    6、复杂操作，插入，添加，更新，删除，每步操作查看结果正确性
     */

    @Test
    public void testAppend2ViaQueryAll() {
        try {
            String clearData = "CLEAR DATA;";
            SessionExecuteSqlResult res = session.executeSql(clearData);

            execute("insert2.json", TYPE.INSERT);
            execute("add2.json", TYPE.APPEND);

            execute("delete.json", TYPE.DELETE);
            String ans = "{\"queries\":[{\"name\": \"archive_file_tracked.ann\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUp\",\"description\": \"dspNewUp\",\"category\": [\"zat3\",\"zat4\"]}, \"values\": [[1359788300001,55.0],[1359788400000,11.0],[1359788400001,44.0],[1359788410001,66.0]]},{\"name\": \"archive_file_tracked.bcc\", \"tags\": {\"data_center\" : [\"DC1\"],\"host\" : [\"server1\"]},\"annotation\": {\"title\": \"titleNewUpbcc\",\"description\": \"dspNewUpbcc\",\"category\": [\"cat2\",\"cat3\",\"cat4\"]}, \"values\": [[1359788400000,77.0]]}]}";
            executeAndCompare("testAppend2ViaQueryAll.json", ans, TYPE.QUERYALL);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

}
