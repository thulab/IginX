package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.rest.MetricsResource;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.ibm.icu.impl.UCaseProps;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import static java.lang.reflect.Method.getMethod;
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

    public String execute(String fileName, TYPE type, DataType dataType) throws Exception {
        String ret = new String();
        String curlArray = orderGen(fileName, type);
        Process process = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(curlArray.split(" "));
            if(dataType.equals(DataType.DOUBLE))
                processBuilder.directory(new File("./src/test/resources/restAnnotation/doubleType"));
            if(dataType.equals(DataType.LONG))
                processBuilder.directory(new File("./src/test/resources/restAnnotation/longType"));
            if(dataType.equals(DataType.BINARY))
                processBuilder.directory(new File("./src/test/resources/restAnnotation/binaryType"));
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

//    @Before
    public void insertData(DataType dataType) {
        try {
            execute("insert.json", TYPE.INSERT, dataType);
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

    public void executeAndCompare(String json, String output, TYPE type, DataType dataType) {
        try {
            String result = execute(json, type, dataType);
            assertEquals(output, removeSpecialChar(result));
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public String ansFromFile(String fileName, DataType dataType) {
        String ret = new String();
        switch (dataType) {
            case DOUBLE:
                fileName = "./src/test/resources/restAnnotation/doubleType/ans/" + fileName;
                break;
            case LONG:
                fileName = "./src/test/resources/restAnnotation/longType/ans/" + fileName;
                break;
            case BINARY:
                fileName = "./src/test/resources/restAnnotation/binaryType/ans/" + fileName;
                break;
        }
        fileName += ".json";

        File file = new File(fileName);
        try {
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line;
            while((line = br.readLine()) != null){
                ret += line;
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
        return removeSpecialChar(ret);
    }

    /**
     * 去除字符串中的空格、回车、换行符、制表符等
     * @param str
     * @return
     */
    public String removeSpecialChar(String str){
        String s = "";
        if(str != null){
            // 定义含特殊字符的正则表达式
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            s = m.replaceAll("");
        }
        return s;
    }

    private String getMethodName() {
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement e = stacktrace[2];
        return e.getMethodName();
    }

    public String getAns(String fileName, DataType dataType) {
        String ans = null;
        switch (dataType) {
            case DOUBLE:
                ans = ansFromFile(fileName,DataType.DOUBLE);
                break;
            case LONG:
                ans = ansFromFile(fileName,DataType.LONG);
                break;
            case BINARY:
                ans = ansFromFile(fileName,DataType.BINARY);
                break;
        }
        return ans;
    }

    public void clearDataMen() {
        try {
            String clearData = "CLEAR DATA;";
            session.executeSql(clearData);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    @Test
    public void testDoubleType() {
        DataType dataType = DataType.DOUBLE;

        /*
        1、查询anntation信息
        */
        testQueryAnno(dataType); clearDataMen();

        /*
        2、查询数据以及annotation信息
        */
        testQueryAll(dataType); clearDataMen();

        /*
        3、对每个修改操作单独测试，并通过两种查询分别验证正确性：
        3.1、测试 add（增加标签操作），通过queryAnno以及queryAll两种方法测试
        3.2、测试 update（更新标签操作），通过queryAnno以及queryAll两种方法测试
        3.3、测试 delete（删除标签操作），通过queryAnno以及queryAll两种方法测试
        */
        testAppendViaQueryAnno(dataType); clearDataMen();
        testAppendViaQueryAll(dataType); clearDataMen();
        testUpdateViaQueryAll(dataType); clearDataMen();
        testUpdateViaQueryAnno(dataType); clearDataMen();
        testDeleteViaQueryAll(dataType); clearDataMen();
        testDeleteViaQueryAnno(dataType); clearDataMen();

        /*
        4、测试重复性操作操作，查看结果正确性
        4.1、测试添加相同category，通过queryAnno以及queryAll两种方法测试
        4.2、测试不断更新相同结果的category，通过queryAnno以及queryAll两种方法测试
        */
        testDuplicateAppendViaQueryAnno(dataType); clearDataMen();
        testDuplicateAppendViaQueryAll(dataType); clearDataMen();
        testDuplicateUpdateViaQueryAnno(dataType); clearDataMen();
        testDuplicateDeleteViaQueryAll(dataType); clearDataMen();

        /*
        5、逻辑上重复的操作，如更新结果与原category相同，查看结果正确性
        */
        testSameUpdateViaQueryAll(dataType); clearDataMen();
        testSameAppendViaQueryAll(dataType); clearDataMen();

        /*
        6、复杂操作，插入，添加，更新，删除，每步操作查看结果正确性
        */
        testAppend2ViaQueryAll(dataType); clearDataMen();
    }

    @Test
    public void testLongType() {
        DataType dataType = DataType.LONG;

        /*
        1、查询anntation信息
        */
        testQueryAnno(dataType); clearDataMen();

        /*
        2、查询数据以及annotation信息
        */
        testQueryAll(dataType); clearDataMen();

        /*
        3、对每个修改操作单独测试，并通过两种查询分别验证正确性：
        3.1、测试 add（增加标签操作），通过queryAnno以及queryAll两种方法测试
        3.2、测试 update（更新标签操作），通过queryAnno以及queryAll两种方法测试
        3.3、测试 delete（删除标签操作），通过queryAnno以及queryAll两种方法测试
        */
        testAppendViaQueryAnno(dataType); clearDataMen();
        testAppendViaQueryAll(dataType); clearDataMen();
        testUpdateViaQueryAll(dataType); clearDataMen();
        testUpdateViaQueryAnno(dataType); clearDataMen();
        testDeleteViaQueryAll(dataType); clearDataMen();
        testDeleteViaQueryAnno(dataType); clearDataMen();

        /*
        4、测试重复性操作操作，查看结果正确性
        4.1、测试添加相同category，通过queryAnno以及queryAll两种方法测试
        4.2、测试不断更新相同结果的category，通过queryAnno以及queryAll两种方法测试
        */
        testDuplicateAppendViaQueryAnno(dataType); clearDataMen();
        testDuplicateAppendViaQueryAll(dataType); clearDataMen();
        testDuplicateUpdateViaQueryAnno(dataType); clearDataMen();
        testDuplicateDeleteViaQueryAll(dataType); clearDataMen();

        /*
        5、逻辑上重复的操作，如更新结果与原category相同，查看结果正确性
        */
        testSameUpdateViaQueryAll(dataType); clearDataMen();
        testSameAppendViaQueryAll(dataType); clearDataMen();

        /*
        6、复杂操作，插入，添加，更新，删除，每步操作查看结果正确性
        */
        testAppend2ViaQueryAll(dataType); clearDataMen();
    }

    @Test
    public void testBinaryType() {
        DataType dataType = DataType.BINARY;

        /*
        1、查询anntation信息
        */
        testQueryAnno(dataType); clearDataMen();

        /*
        2、查询数据以及annotation信息
        */
        testQueryAll(dataType); clearDataMen();

        /*
        3、对每个修改操作单独测试，并通过两种查询分别验证正确性：
        3.1、测试 add（增加标签操作），通过queryAnno以及queryAll两种方法测试
        3.2、测试 update（更新标签操作），通过queryAnno以及queryAll两种方法测试
        3.3、测试 delete（删除标签操作），通过queryAnno以及queryAll两种方法测试
        */
        testAppendViaQueryAnno(dataType); clearDataMen();
        testAppendViaQueryAll(dataType); clearDataMen();
        testUpdateViaQueryAll(dataType); clearDataMen();
        testUpdateViaQueryAnno(dataType); clearDataMen();
        testDeleteViaQueryAll(dataType); clearDataMen();
        testDeleteViaQueryAnno(dataType); clearDataMen();

        /*
        4、测试重复性操作操作，查看结果正确性
        4.1、测试添加相同category，通过queryAnno以及queryAll两种方法测试
        4.2、测试不断更新相同结果的category，通过queryAnno以及queryAll两种方法测试
        */
        testDuplicateAppendViaQueryAnno(dataType); clearDataMen();
        testDuplicateAppendViaQueryAll(dataType); clearDataMen();
        testDuplicateUpdateViaQueryAnno(dataType); clearDataMen();
        testDuplicateDeleteViaQueryAll(dataType); clearDataMen();

        /*
        5、逻辑上重复的操作，如更新结果与原category相同，查看结果正确性
        */
        testSameUpdateViaQueryAll(dataType); clearDataMen();
        testSameAppendViaQueryAll(dataType); clearDataMen();

        /*
        6、复杂操作，插入，添加，更新，删除，每步操作查看结果正确性
        */
        testAppend2ViaQueryAll(dataType); clearDataMen();
    }

    @Test
    public void testAllAppend() {
        for(int i=0;i<3;i++) {
            DataType dataType = null;
            if(i==0) dataType = DataType.DOUBLE;
            if(i==1) dataType = DataType.LONG;
            if(i==2) dataType = DataType.BINARY;

            testAppendViaQueryAnno(dataType); clearDataMen();
            testAppendViaQueryAll(dataType); clearDataMen();

            testDuplicateAppendViaQueryAnno(dataType); clearDataMen();
            testDuplicateAppendViaQueryAll(dataType); clearDataMen();

            testAppend2ViaQueryAll(dataType); clearDataMen();
        }
    }

    @Test
    public void testAllDelete() {
        for(int i=0;i<3;i++) {
            DataType dataType = null;
            if(i==0) dataType = DataType.DOUBLE;
            if(i==1) dataType = DataType.LONG;
            if(i==2) dataType = DataType.BINARY;

            testDeleteViaQueryAll(dataType); clearDataMen();
            testDeleteViaQueryAnno(dataType); clearDataMen();

            testDuplicateDeleteViaQueryAll(dataType); clearDataMen();
        }
    }

    @Test
    public void testAllUpdate() {
        for(int i=0;i<3;i++) {
            DataType dataType = null;
            if(i==0) dataType = DataType.DOUBLE;
            if(i==1) dataType = DataType.LONG;
            if(i==2) dataType = DataType.BINARY;

            testUpdateViaQueryAll(dataType); clearDataMen();
            testUpdateViaQueryAnno(dataType); clearDataMen();

            testDuplicateUpdateViaQueryAnno(dataType); clearDataMen();

            testSameUpdateViaQueryAll(dataType); clearDataMen();
        }
    }

    public void testQueryAnno(DataType dataType) {
        insertData(dataType);
        String ans = getAns(getMethodName(),dataType);
        executeAndCompare("queryAnno.json", ans, TYPE.QUERYANNO, dataType);
        clearDataMen();
    }

    public void testQueryAll(DataType dataType) {
        insertData(dataType);
        String ans = getAns(getMethodName(),dataType);
        executeAndCompare("queryData.json", ans, TYPE.QUERYALL, DataType.DOUBLE);
    }

    public void testAppendViaQueryAnno(DataType dataType) {
        insertData(dataType);
        try {
            execute("add.json", TYPE.APPEND, DataType.DOUBLE);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryAppendViaQueryAnno.json", ans, TYPE.QUERYANNO, DataType.DOUBLE);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testAppendViaQueryAll(DataType dataType) {
        insertData(dataType);
        try {
            execute("add.json", TYPE.APPEND, dataType);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryAppendViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testUpdateViaQueryAll(DataType dataType) {
        insertData(dataType);
        try {
            execute("update.json", TYPE.UPDATE, dataType);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryUpdateViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testUpdateViaQueryAnno(DataType dataType) {
        insertData(dataType);
        try {
            execute("update.json", TYPE.UPDATE, dataType);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryUpdateViaQueryAnno.json", ans, TYPE.QUERYANNO, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testDeleteViaQueryAll(DataType dataType) {
        insertData(dataType);
        try {
            execute("delete.json", TYPE.DELETE, dataType);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("deleteViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testDeleteViaQueryAnno(DataType dataType) {
        insertData(dataType);
        try {
            execute("delete.json", TYPE.DELETE, dataType);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("deleteViaQueryAnno.json", ans, TYPE.QUERYANNO, dataType);
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

    public void testDuplicateAppendViaQueryAnno(DataType dataType) {
        try {
            execute("insert2.json", TYPE.INSERT, dataType);
            execute("add2.json", TYPE.APPEND, dataType);
            execute("add2.json", TYPE.APPEND, dataType);

            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("testAppend2ViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testDuplicateAppendViaQueryAll(DataType dataType) {
        insertData(dataType);
        try {
            execute("add.json", TYPE.APPEND, dataType);
            execute("add.json", TYPE.APPEND, dataType);

            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryAppendViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testDuplicateUpdateViaQueryAll(DataType dataType) {
        insertData(dataType);
        try {
            execute("update.json", TYPE.UPDATE, dataType);
            execute("update.json", TYPE.UPDATE, dataType);

            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryUpdateViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testDuplicateUpdateViaQueryAnno(DataType dataType) {
        insertData(dataType);
        try {
            execute("update.json", TYPE.UPDATE, dataType);
            execute("update.json", TYPE.UPDATE, dataType);

            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryUpdateViaQueryAnno.json", ans, TYPE.QUERYANNO, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testDuplicateDeleteViaQueryAll(DataType dataType) {
        insertData(dataType);
        try {
            execute("delete.json", TYPE.DELETE, dataType);
            execute("delete.json", TYPE.DELETE, dataType);

            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("deleteViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    /*
    5、逻辑上重复的操作，如更新结果与原category相同，查看结果正确性
     */

    public void testSameUpdateViaQueryAll(DataType dataType) {
        insertData(dataType);
        try {
            execute("updateSame.json", TYPE.UPDATE, dataType);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryData.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    public void testSameAppendViaQueryAll(DataType dataType) {
        insertData(dataType);
        try {
            execute("addSame.json", TYPE.APPEND, dataType);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("queryAppendViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

    /*
    6、复杂操作，插入，添加，更新，删除，每步操作查看结果正确性
     */

    public void testAppend2ViaQueryAll(DataType dataType) {
        try {
            execute("insert2.json", TYPE.INSERT, dataType);
            execute("add2.json", TYPE.APPEND, dataType);

            execute("delete.json", TYPE.DELETE, dataType);
            String ans = getAns(getMethodName(),dataType);
            executeAndCompare("testAppend2ViaQueryAll.json", ans, TYPE.QUERYALL, dataType);
        } catch (Exception e) {
            LOGGER.error("Error occurred during execution ", e);
            fail();
        }
    }

}
