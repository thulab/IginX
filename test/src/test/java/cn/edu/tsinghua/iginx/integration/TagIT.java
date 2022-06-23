package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TagIT {

    private static final Logger logger = LoggerFactory.getLogger(SQLSessionIT.class);

    private static Session session;

    @BeforeClass
    public static void setUp() {
        session = new Session("127.0.0.1", 6888, "root", "root");
        try {
            session.openSession();
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            session.closeSession();
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    @Before
    public void insertData() throws ExecutionException, SessionException {
        String[] insertStatements = ("insert into ln.wf02 (time, s, v) values (100, true, \"v1\");\n" +
                "insert into ln.wf02[t1=v1] (time, s, v) values (400, false, \"v4\");\n" +
                "insert into ln.wf02[t1=v1,t2=v2] (time, v) values (800, \"v8\");\n" +
                "insert into ln.wf03 (time, s[t1=vv1,t2=v2], v[t1=vv11]) values (1600, true, 16);\n" +
                "insert into ln.wf03 (time, s[t1=v1,t2=vv2], v[t1=v1], v[t1=vv11]) values (3200, true, \"v32\", 32);").split("\n");

        for (String insertStatement: insertStatements) {
            SessionExecuteSqlResult res = session.executeSql(insertStatement);
            if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
                logger.error("Insert date execute fail. Caused by: {}.", res.getParseErrorMsg());
                fail();
            }
        }
    }

    @After
    public void clearData() throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = session.executeSql(clearData);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Clear date execute fail. Caused by: {}.", res.getParseErrorMsg());
            fail();
        }
    }

    private void executeAndCompare(String statement, String exceptOutput) {
        String actualOutput = execute(statement);
        assertEquals(exceptOutput, actualOutput);
    }

    private String execute(String statement) {
        logger.info("Execute Statement: \"{}\"", statement);

        SessionExecuteSqlResult res = null;
        try {
            res = session.executeSql(statement);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by:", statement, e);
            fail();
        }

        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
            fail();
            return "";
        }

        return res.getResultInString(false, "");
    }

    @Test
    public void testCountPoints() {
        String statement = "COUNT POINTS;";
        String excepted = "Points num: 10\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testCountPath() {
        String statement = "SELECT COUNT(*) FROM ln;";
        String excepted = "ResultSets:\n" +
                "+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n" +
                "|count(ln.wf02.s)|count(ln.wf02.s{t1=v1})|count(ln.wf02.v)|count(ln.wf02.v{t1=v1,t2=v2})|count(ln.wf02.v{t1=v1})|count(ln.wf03.s{t1=v1,t2=vv2})|count(ln.wf03.s{t1=vv1,t2=v2})|count(ln.wf03.v{t1=v1})|count(ln.wf03.v{t1=vv11})|\n" +
                "+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n" +
                "|               1|                      1|               1|                            1|                      1|                             1|                             1|                      1|                        2|\n" +
                "+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testBasicQuery() {
        String statement = "SELECT * FROM ln;";
        String excepted = "ResultSets:\n" +
                "+----+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n" +
                "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf02.v|ln.wf02.v{t1=v1,t2=v2}|ln.wf02.v{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|ln.wf03.v{t1=v1}|ln.wf03.v{t1=vv11}|\n" +
                "+----+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n" +
                "| 100|     true|            null|       v1|                  null|            null|                   null|                   null|            null|              null|\n" +
                "| 400|     null|           false|     null|                  null|              v4|                   null|                   null|            null|              null|\n" +
                "| 800|     null|            null|     null|                    v8|            null|                   null|                   null|            null|              null|\n" +
                "|1600|     null|            null|     null|                  null|            null|                   null|                   true|            null|                16|\n" +
                "|3200|     null|            null|     null|                  null|            null|                   true|                   null|             v32|                32|\n" +
                "+----+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n" +
                "Total line number = 5\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testQueryWithoutTags() {
        String statement = "SELECT s FROM ln.*;";
        String excepted = "ResultSets:\n" +
                "+----+---------+----------------+-----------------------+-----------------------+\n" +
                "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n" +
                "+----+---------+----------------+-----------------------+-----------------------+\n" +
                "| 100|     true|            null|                   null|                   null|\n" +
                "| 400|     null|           false|                   null|                   null|\n" +
                "|1600|     null|            null|                   null|                   true|\n" +
                "|3200|     null|            null|                   true|                   null|\n" +
                "+----+---------+----------------+-----------------------+-----------------------+\n" +
                "Total line number = 4\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testQueryWithTag() {
        String statement = "SELECT s FROM ln.* with t1=v1;";
        String excepted = "ResultSets:\n" +
                "+----+----------------+-----------------------+\n" +
                "|Time|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|\n" +
                "+----+----------------+-----------------------+\n" +
                "| 400|           false|                   null|\n" +
                "|3200|            null|                   true|\n" +
                "+----+----------------+-----------------------+\n" +
                "Total line number = 2\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testQueryWithMultiTags() {
        String statement = "SELECT s FROM ln.* with t1=v1 OR t2=v2;";
        String excepted = "ResultSets:\n" +
                "+----+----------------+-----------------------+-----------------------+\n" +
                "|Time|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n" +
                "+----+----------------+-----------------------+-----------------------+\n" +
                "| 400|           false|                   null|                   null|\n" +
                "|1600|            null|                   null|                   true|\n" +
                "|3200|            null|                   true|                   null|\n" +
                "+----+----------------+-----------------------+-----------------------+\n" +
                "Total line number = 3\n";
        executeAndCompare(statement, excepted);

        statement = "SELECT s FROM ln.* with t1=v1 AND t2=vv2;";
        excepted = "ResultSets:\n" +
                "+----+-----------------------+\n" +
                "|Time|ln.wf03.s{t1=v1,t2=vv2}|\n" +
                "+----+-----------------------+\n" +
                "|3200|                   true|\n" +
                "+----+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testQueryWithWildcardTag() {
        String statement = "SELECT s FROM ln.* with t2=*;";
        String excepted = "ResultSets:\n" +
                "+----+-----------------------+-----------------------+\n" +
                "|Time|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n" +
                "+----+-----------------------+-----------------------+\n" +
                "|1600|                   null|                   true|\n" +
                "|3200|                   true|                   null|\n" +
                "+----+-----------------------+-----------------------+\n" +
                "Total line number = 2\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testQueryWithAggregate() {
        String statement = "SELECT sum(v) FROM ln.wf03 with t1=vv11;";
        String excepted = "ResultSets:\n" +
                "+-----------------------+\n" +
                "|sum(ln.wf03.v{t1=vv11})|\n" +
                "+-----------------------+\n" +
                "|                     48|\n" +
                "+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, excepted);

        statement = "SELECT max(v) FROM ln.wf03 with t1=vv11;";
        excepted = "ResultSets:\n" +
                "+-----------------------+\n" +
                "|max(ln.wf03.v{t1=vv11})|\n" +
                "+-----------------------+\n" +
                "|                     32|\n" +
                "+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, excepted);

        statement = "SELECT min(v) FROM ln.wf03 with t1=vv11;";
        excepted = "ResultSets:\n" +
                "+-----------------------+\n" +
                "|min(ln.wf03.v{t1=vv11})|\n" +
                "+-----------------------+\n" +
                "|                     16|\n" +
                "+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, excepted);

        statement = "SELECT avg(v) FROM ln.wf03 with t1=vv11;";
        excepted = "ResultSets:\n" +
                "+-----------------------+\n" +
                "|avg(ln.wf03.v{t1=vv11})|\n" +
                "+-----------------------+\n" +
                "|                   24.0|\n" +
                "+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, excepted);

        statement = "SELECT count(v) FROM ln.wf03 with t1=vv11;";
        excepted = "ResultSets:\n" +
                "+-------------------------+\n" +
                "|count(ln.wf03.v{t1=vv11})|\n" +
                "+-------------------------+\n" +
                "|                        2|\n" +
                "+-------------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testClearData() {
        String clearData = "CLEAR DATA;";
        execute(clearData);

        String countPoints = "COUNT POINTS;";
        String excepted = "Points num: 0\n";
        executeAndCompare(countPoints, excepted);

        String showTimeSeries = "SELECT * FROM *;";
        excepted = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";
        executeAndCompare(showTimeSeries, excepted);
    }

}
