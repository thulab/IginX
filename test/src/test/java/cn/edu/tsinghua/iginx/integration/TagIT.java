package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TagIT {

    private static final Logger logger = LoggerFactory.getLogger(TagIT.class);

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
            "insert into ln.wf03 (time, s[t1=v1,t2=vv2], v[t1=v1], v[t1=vv11]) values (3200, true, 16, 32);").split("\n");

        for (String insertStatement : insertStatements) {
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

    private void executeAndCompare(String statement, String expectedOutput) {
        String actualOutput = execute(statement);
        assertEquals(expectedOutput, actualOutput);
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
    public void testShowTimeSeriesWithTags() {
        String statement = "SHOW TIME SERIES;";
        String expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|              ln.wf02.s| BOOLEAN|\n"
                + "|       ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|              ln.wf02.v|  BINARY|\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                + "|ln.wf03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                + "|       ln.wf03.v{t1=v1}|    LONG|\n"
                + "|     ln.wf03.v{t1=vv11}|    LONG|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 9\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES limit 6;";
        expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|              ln.wf02.s| BOOLEAN|\n"
                + "|       ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|              ln.wf02.v|  BINARY|\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 6\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES limit 3 offset 3;";
        expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES limit 3, 3;";
        expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ln.wf02.*;";
        expected =
            "Time series:\n"
                + "+----------------------+--------+\n"
                + "|                  Path|DataType|\n"
                + "+----------------------+--------+\n"
                + "|             ln.wf02.s| BOOLEAN|\n"
                + "|      ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|             ln.wf02.v|  BINARY|\n"
                + "|      ln.wf02.v{t1=v1}|  BINARY|\n"
                + "|ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "+----------------------+--------+\n"
                + "Total line number = 5\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ln.wf02.* limit 3 offset 2;";
        expected =
            "Time series:\n"
                + "+----------------------+--------+\n"
                + "|                  Path|DataType|\n"
                + "+----------------------+--------+\n"
                + "|             ln.wf02.v|  BINARY|\n"
                + "|      ln.wf02.v{t1=v1}|  BINARY|\n"
                + "|ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "+----------------------+--------+\n"
                + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ln.wf02.*, ln.wf03.*;";
        expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|              ln.wf02.s| BOOLEAN|\n"
                + "|       ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|              ln.wf02.v|  BINARY|\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                + "|ln.wf03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                + "|       ln.wf03.v{t1=v1}|    LONG|\n"
                + "|     ln.wf03.v{t1=vv11}|    LONG|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 9\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ln.wf02.* with t1=v1;";
        expected =
            "Time series:\n"
                + "+----------------------+--------+\n"
                + "|                  Path|DataType|\n"
                + "+----------------------+--------+\n"
                + "|      ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|      ln.wf02.v{t1=v1}|  BINARY|\n"
                + "|ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "+----------------------+--------+\n"
                + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ln.wf02.* with t1=v1 limit 2 offset 1;";
        expected =
            "Time series:\n"
                + "+----------------------+--------+\n"
                + "|                  Path|DataType|\n"
                + "+----------------------+--------+\n"
                + "|      ln.wf02.v{t1=v1}|  BINARY|\n"
                + "|ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "+----------------------+--------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ln.wf02.* with_precise t1=v1;";
        expected =
            "Time series:\n"
                + "+----------------+--------+\n"
                + "|            Path|DataType|\n"
                + "+----------------+--------+\n"
                + "|ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|ln.wf02.v{t1=v1}|  BINARY|\n"
                + "+----------------+--------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ln.wf02.* with_precise t1=v1 AND t2=v2;";
        expected =
            "Time series:\n"
                + "+----------------------+--------+\n"
                + "|                  Path|DataType|\n"
                + "+----------------------+--------+\n"
                + "|ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "+----------------------+--------+\n"
                + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ln.wf02.* with t1=v1 AND t2=v2;";
        expected =
            "Time series:\n"
                + "+----------------------+--------+\n"
                + "|                  Path|DataType|\n"
                + "+----------------------+--------+\n"
                + "|ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "+----------------------+--------+\n"
                + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES WITHOUT TAG;";
        expected =
            "Time series:\n"
                + "+---------+--------+\n"
                + "|     Path|DataType|\n"
                + "+---------+--------+\n"
                + "|ln.wf02.s| BOOLEAN|\n"
                + "|ln.wf02.v|  BINARY|\n"
                + "+---------+--------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testCountPoints() {
        String statement = "COUNT POINTS;";
        String expected = "Points num: 10\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testCountPath() {
        String statement = "SELECT COUNT(*) FROM ln;";
        String expected = "ResultSets:\n" +
            "+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n" +
            "|count(ln.wf02.s)|count(ln.wf02.s{t1=v1})|count(ln.wf02.v)|count(ln.wf02.v{t1=v1,t2=v2})|count(ln.wf02.v{t1=v1})|count(ln.wf03.s{t1=v1,t2=vv2})|count(ln.wf03.s{t1=vv1,t2=v2})|count(ln.wf03.v{t1=v1})|count(ln.wf03.v{t1=vv11})|\n" +
            "+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n" +
            "|               1|                      1|               1|                            1|                      1|                             1|                             1|                      1|                        2|\n" +
            "+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testBasicQuery() {
        String statement = "SELECT * FROM ln;";
        String expected = "ResultSets:\n" +
            "+----+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n" +
            "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf02.v|ln.wf02.v{t1=v1,t2=v2}|ln.wf02.v{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|ln.wf03.v{t1=v1}|ln.wf03.v{t1=vv11}|\n" +
            "+----+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n" +
            "| 100|     true|            null|       v1|                  null|            null|                   null|                   null|            null|              null|\n" +
            "| 400|     null|           false|     null|                  null|              v4|                   null|                   null|            null|              null|\n" +
            "| 800|     null|            null|     null|                    v8|            null|                   null|                   null|            null|              null|\n" +
            "|1600|     null|            null|     null|                  null|            null|                   null|                   true|            null|                16|\n" +
            "|3200|     null|            null|     null|                  null|            null|                   true|                   null|              16|                32|\n" +
            "+----+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n" +
            "Total line number = 5\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testQueryWithoutTags() {
        String statement = "SELECT s FROM ln.*;";
        String expected = "ResultSets:\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "| 100|     true|            null|                   null|                   null|\n" +
            "| 400|     null|           false|                   null|                   null|\n" +
            "|1600|     null|            null|                   null|                   true|\n" +
            "|3200|     null|            null|                   true|                   null|\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ln.* WITHOUT TAG;";
        expected =
            "ResultSets:\n"
                + "+----+---------+\n"
                + "|Time|ln.wf02.s|\n"
                + "+----+---------+\n"
                + "| 100|     true|\n"
                + "+----+---------+\n"
                + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT v FROM ln.*;";
        expected =
            "ResultSets:\n"
                + "+----+---------+----------------------+----------------+----------------+------------------+\n"
                + "|Time|ln.wf02.v|ln.wf02.v{t1=v1,t2=v2}|ln.wf02.v{t1=v1}|ln.wf03.v{t1=v1}|ln.wf03.v{t1=vv11}|\n"
                + "+----+---------+----------------------+----------------+----------------+------------------+\n"
                + "| 100|       v1|                  null|            null|            null|              null|\n"
                + "| 400|     null|                  null|              v4|            null|              null|\n"
                + "| 800|     null|                    v8|            null|            null|              null|\n"
                + "|1600|     null|                  null|            null|            null|                16|\n"
                + "|3200|     null|                  null|            null|              16|                32|\n"
                + "+----+---------+----------------------+----------------+----------------+------------------+\n"
                + "Total line number = 5\n";
        executeAndCompare(statement, expected);

        statement = "SELECT v FROM ln.* WITHOUT TAG;";
        expected =
            "ResultSets:\n"
                + "+----+---------+\n"
                + "|Time|ln.wf02.v|\n"
                + "+----+---------+\n"
                + "| 100|       v1|\n"
                + "+----+---------+\n"
                + "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testQueryWithTag() {
        String statement = "SELECT s FROM ln.* with t1=v1;";
        String expected = "ResultSets:\n" +
            "+----+----------------+-----------------------+\n" +
            "|Time|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|\n" +
            "+----+----------------+-----------------------+\n" +
            "| 400|           false|                   null|\n" +
            "|3200|            null|                   true|\n" +
            "+----+----------------+-----------------------+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ln.* with_precise t1=v1;";
        expected =
            "ResultSets:\n"
                + "+----+----------------+\n"
                + "|Time|ln.wf02.s{t1=v1}|\n"
                + "+----+----------------+\n"
                + "| 400|           false|\n"
                + "+----+----------------+\n"
                + "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testQueryWithMultiTags() {
        String statement = "SELECT s FROM ln.* with t1=v1 OR t2=v2;";
        String expected = "ResultSets:\n" +
            "+----+----------------+-----------------------+-----------------------+\n" +
            "|Time|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n" +
            "+----+----------------+-----------------------+-----------------------+\n" +
            "| 400|           false|                   null|                   null|\n" +
            "|1600|            null|                   null|                   true|\n" +
            "|3200|            null|                   true|                   null|\n" +
            "+----+----------------+-----------------------+-----------------------+\n" +
            "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ln.* with t1=v1 AND t2=vv2;";
        expected = "ResultSets:\n" +
            "+----+-----------------------+\n" +
            "|Time|ln.wf03.s{t1=v1,t2=vv2}|\n" +
            "+----+-----------------------+\n" +
            "|3200|                   true|\n" +
            "+----+-----------------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ln.* with_precise t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
        expected =
            "ResultSets:\n"
                + "+----+-----------------------+-----------------------+\n"
                + "|Time|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n"
                + "+----+-----------------------+-----------------------+\n"
                + "|1600|                   null|                   true|\n"
                + "|3200|                   true|                   null|\n"
                + "+----+-----------------------+-----------------------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ln.* with_precise t1=v1;";
        expected =
            "ResultSets:\n"
                + "+----+----------------+\n"
                + "|Time|ln.wf02.s{t1=v1}|\n"
                + "+----+----------------+\n"
                + "| 400|           false|\n"
                + "+----+----------------+\n"
                + "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testDeleteWithTag() {
        String statement = "SELECT s FROM ln.*;";
        String expected = "ResultSets:\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "| 100|     true|            null|                   null|                   null|\n" +
            "| 400|     null|           false|                   null|                   null|\n" +
            "|1600|     null|            null|                   null|                   true|\n" +
            "|3200|     null|            null|                   true|                   null|\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "DELETE FROM ln.*.s WHERE time > 10 WITH t1=v1;";
        execute(statement);

        statement = "SELECT s FROM ln.*;";
        expected =
            "ResultSets:\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "| 100|     true|            null|                   null|                   null|\n"
                + "|1600|     null|            null|                   null|                   true|\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testDeleteWithMultiTags() {
        String statement = "SELECT s FROM ln.*;";
        String expected = "ResultSets:\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "| 100|     true|            null|                   null|                   null|\n" +
            "| 400|     null|           false|                   null|                   null|\n" +
            "|1600|     null|            null|                   null|                   true|\n" +
            "|3200|     null|            null|                   true|                   null|\n" +
            "+----+---------+----------------+-----------------------+-----------------------+\n" +
            "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "DELETE FROM ln.*.s WHERE time > 10 WITH t1=v1 AND t2=vv2;";
        execute(statement);

        statement = "SELECT s FROM ln.*;";
        expected =
            "ResultSets:\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "| 100|     true|            null|                   null|                   null|\n"
                + "| 400|     null|           false|                   null|                   null|\n"
                + "|1600|     null|            null|                   null|                   true|\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "DELETE FROM ln.*.s WHERE time > 10 WITH_PRECISE t1=v1;";
        execute(statement);

        statement = "SELECT s FROM ln.*;";
        expected =
            "ResultSets:\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "| 100|     true|            null|                   null|                   null|\n"
                + "|1600|     null|            null|                   null|                   true|\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "DELETE FROM ln.*.s WHERE time > 10 WITH t1=v1 OR t2=v2;";
        execute(statement);

        statement = "SELECT s FROM ln.*;";
        expected =
            "ResultSets:\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "|Time|ln.wf02.s|ln.wf02.s{t1=v1}|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "| 100|     true|            null|                   null|                   null|\n"
                + "+----+---------+----------------+-----------------------+-----------------------+\n"
                + "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testDeleteTSWithTag() {
        String showTimeSeries = "SHOW TIME SERIES;";
        String expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|              ln.wf02.s| BOOLEAN|\n"
                + "|       ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|              ln.wf02.v|  BINARY|\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                + "|ln.wf03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                + "|       ln.wf03.v{t1=v1}|    LONG|\n"
                + "|     ln.wf03.v{t1=vv11}|    LONG|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 9\n";
        executeAndCompare(showTimeSeries, expected);

        String deleteTimeSeries = "DELETE TIME SERIES ln.*.s WITH t1=v1";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES;";
        expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|              ln.wf02.s| BOOLEAN|\n"
                + "|              ln.wf02.v|  BINARY|\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                + "|       ln.wf03.v{t1=v1}|    LONG|\n"
                + "|     ln.wf03.v{t1=vv11}|    LONG|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 7\n";
        executeAndCompare(showTimeSeries, expected);

        String showTimeSeriesData = "SELECT s FROM ln.* WITH t1=v1;";
        expected = "ResultSets:\n" +
            "+----+\n" +
            "|Time|\n" +
            "+----+\n" +
            "+----+\n" +
            "Empty set.\n";
        executeAndCompare(showTimeSeriesData, expected);

        deleteTimeSeries = "DELETE TIME SERIES ln.*.v WITH_PRECISE t1=v1";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES;";
        expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|              ln.wf02.s| BOOLEAN|\n"
                + "|              ln.wf02.v|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                + "|     ln.wf03.v{t1=vv11}|    LONG|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 5\n";
        executeAndCompare(showTimeSeries, expected);

        showTimeSeriesData = "SELECT v FROM ln.* WITH t1=v1;";
        expected =
            "ResultSets:\n"
                + "+----+----------------------+\n"
                + "|Time|ln.wf02.v{t1=v1,t2=v2}|\n"
                + "+----+----------------------+\n"
                + "| 800|                    v8|\n"
                + "+----+----------------------+\n"
                + "Total line number = 1\n";
        executeAndCompare(showTimeSeriesData, expected);
    }

    @Test
    public void testDeleteTSWithMultiTags() {
        String showTimeSeries = "SHOW TIME SERIES;";
        String expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|              ln.wf02.s| BOOLEAN|\n"
                + "|       ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|              ln.wf02.v|  BINARY|\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "| ln.wf02.v{t1=v1,t2=v2}|  BINARY|\n"
                + "|ln.wf03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                + "|ln.wf03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                + "|       ln.wf03.v{t1=v1}|    LONG|\n"
                + "|     ln.wf03.v{t1=vv11}|    LONG|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 9\n";
        executeAndCompare(showTimeSeries, expected);

        String deleteTimeSeries = "DELETE TIME SERIES ln.*.v WITH t1=v1 AND t2=v2;";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES;";
        expected =
            "Time series:\n"
                + "+-----------------------+--------+\n"
                + "|                   Path|DataType|\n"
                + "+-----------------------+--------+\n"
                + "|              ln.wf02.s| BOOLEAN|\n"
                + "|       ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|              ln.wf02.v|  BINARY|\n"
                + "|       ln.wf02.v{t1=v1}|  BINARY|\n"
                + "|ln.wf03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                + "|ln.wf03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                + "|       ln.wf03.v{t1=v1}|    LONG|\n"
                + "|     ln.wf03.v{t1=vv11}|    LONG|\n"
                + "+-----------------------+--------+\n"
                + "Total line number = 8\n";
        executeAndCompare(showTimeSeries, expected);

        String showTimeSeriesData = "SELECT v FROM ln.* WITH t1=v1 AND t2=v2;";
        expected = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";;
        executeAndCompare(showTimeSeriesData, expected);

        deleteTimeSeries = "DELETE TIME SERIES * WITH t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES;";
        expected =
            "Time series:\n"
                + "+------------------+--------+\n"
                + "|              Path|DataType|\n"
                + "+------------------+--------+\n"
                + "|         ln.wf02.s| BOOLEAN|\n"
                + "|  ln.wf02.s{t1=v1}| BOOLEAN|\n"
                + "|         ln.wf02.v|  BINARY|\n"
                + "|  ln.wf02.v{t1=v1}|  BINARY|\n"
                + "|  ln.wf03.v{t1=v1}|    LONG|\n"
                + "|ln.wf03.v{t1=vv11}|    LONG|\n"
                + "+------------------+--------+\n"
                + "Total line number = 6\n";
        executeAndCompare(showTimeSeries, expected);

        showTimeSeriesData = "SELECT * FROM * WITH t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
        expected = "ResultSets:\n" +
            "+----+\n" +
            "|Time|\n" +
            "+----+\n" +
            "+----+\n" +
            "Empty set.\n";;
        executeAndCompare(showTimeSeriesData, expected);
    }

    @Test
    public void testQueryWithWildcardTag() {
        String statement = "SELECT s FROM ln.* with t2=*;";
        String expected = "ResultSets:\n" +
            "+----+-----------------------+-----------------------+\n" +
            "|Time|ln.wf03.s{t1=v1,t2=vv2}|ln.wf03.s{t1=vv1,t2=v2}|\n" +
            "+----+-----------------------+-----------------------+\n" +
            "|1600|                   null|                   true|\n" +
            "|3200|                   true|                   null|\n" +
            "+----+-----------------------+-----------------------+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testQueryWithAggregate() {
        String statement = "SELECT sum(v) FROM ln.wf03 with t1=vv11;";
        String expected = "ResultSets:\n" +
            "+-----------------------+\n" +
            "|sum(ln.wf03.v{t1=vv11})|\n" +
            "+-----------------------+\n" +
            "|                     48|\n" +
            "+-----------------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT max(v) FROM ln.wf03 with t1=vv11;";
        expected = "ResultSets:\n" +
            "+-----------------------+\n" +
            "|max(ln.wf03.v{t1=vv11})|\n" +
            "+-----------------------+\n" +
            "|                     32|\n" +
            "+-----------------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT min(v) FROM ln.wf03 with t1=vv11;";
        expected = "ResultSets:\n" +
            "+-----------------------+\n" +
            "|min(ln.wf03.v{t1=vv11})|\n" +
            "+-----------------------+\n" +
            "|                     16|\n" +
            "+-----------------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT avg(v) FROM ln.wf03 with t1=vv11;";
        expected = "ResultSets:\n" +
            "+-----------------------+\n" +
            "|avg(ln.wf03.v{t1=vv11})|\n" +
            "+-----------------------+\n" +
            "|                   24.0|\n" +
            "+-----------------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT count(v) FROM ln.wf03 with t1=vv11;";
        expected = "ResultSets:\n" +
            "+-------------------------+\n" +
            "|count(ln.wf03.v{t1=vv11})|\n" +
            "+-------------------------+\n" +
            "|                        2|\n" +
            "+-------------------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testAlias() {
        String statement = "SELECT s AS ts FROM ln.wf02;";
        String expected = "ResultSets:\n" +
            "+----+----+---------+\n" +
            "|Time|  ts|ts{t1=v1}|\n" +
            "+----+----+---------+\n" +
            "| 100|true|     null|\n" +
            "| 400|null|    false|\n" +
            "+----+----+---------+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ln.wf02 AS result_set;";
        expected = "ResultSets:\n" +
            "+----+--------------------+---------------------------+\n" +
            "|Time|result_set.ln.wf02.s|result_set.ln.wf02.s{t1=v1}|\n" +
            "+----+--------------------+---------------------------+\n" +
            "| 100|                true|                       null|\n" +
            "| 400|                null|                      false|\n" +
            "+----+--------------------+---------------------------+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s AS ts FROM ln.wf02 AS result_set;";
        expected = "ResultSets:\n" +
            "+----+-------------+--------------------+\n" +
            "|Time|result_set.ts|result_set.ts{t1=v1}|\n" +
            "+----+-------------+--------------------+\n" +
            "| 100|         true|                null|\n" +
            "| 400|         null|               false|\n" +
            "+----+-------------+--------------------+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testSubQuery() {
        String statement = "SELECT SUM(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ln.wf03 with t1=v1);";
        String expected = "ResultSets:\n" +
            "+---------------+\n" +
            "|sum(ts2{t1=v1})|\n" +
            "+---------------+\n" +
            "|             16|\n" +
            "+---------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT AVG(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ln.wf03 with t1=v1);";
        expected = "ResultSets:\n" +
            "+---------------+\n" +
            "|avg(ts2{t1=v1})|\n" +
            "+---------------+\n" +
            "|           16.0|\n" +
            "+---------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT MAX(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ln.wf03 with t1=v1);";
        expected = "ResultSets:\n" +
            "+---------------+\n" +
            "|max(ts2{t1=v1})|\n" +
            "+---------------+\n" +
            "|             16|\n" +
            "+---------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT COUNT(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ln.wf03 with t1=v1);";
        expected = "ResultSets:\n" +
            "+-----------------+\n" +
            "|count(ts2{t1=v1})|\n" +
            "+-----------------+\n" +
            "|                1|\n" +
            "+-----------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testTagInsertWithSubQuery() {
        String query = "SELECT s AS ts1, v AS ts2 FROM ln.wf03 with t1=v1;";
        String expected = "ResultSets:\n" +
            "+----+-----------------+----------+\n" +
            "|Time|ts1{t1=v1,t2=vv2}|ts2{t1=v1}|\n" +
            "+----+-----------------+----------+\n" +
            "|3200|             true|        16|\n" +
            "+----+-----------------+----------+\n" +
            "Total line number = 1\n";
        executeAndCompare(query, expected);

        String insert = "INSERT INTO copy.ln.wf01(TIME, s, v) VALUES (SELECT s AS ts1, v AS ts2 FROM ln.wf03 with t1=v1);";
        execute(insert);

        query = "SELECT s, v FROM copy.ln.wf01;";
        expected =
            "ResultSets:\n"
                + "+----+----------------------------+---------------------+\n"
                + "|Time|copy.ln.wf01.s{t1=v1,t2=vv2}|copy.ln.wf01.v{t1=v1}|\n"
                + "+----+----------------------------+---------------------+\n"
                + "|3200|                        true|                   16|\n"
                + "+----+----------------------------+---------------------+\n"
                + "Total line number = 1\n";
        executeAndCompare(query, expected);

        insert = "INSERT INTO copy.ln.wf02(TIME, s, v[t2=v2]) VALUES (SELECT s AS ts1, v AS ts2 FROM ln.wf03 with t1=v1);";
        execute(insert);

        query = "SELECT s, v FROM copy.ln.wf02;";
        expected =
            "ResultSets:\n"
                + "+----+----------------------------+---------------------------+\n"
                + "|Time|copy.ln.wf02.s{t1=v1,t2=vv2}|copy.ln.wf02.v{t1=v1,t2=v2}|\n"
                + "+----+----------------------------+---------------------------+\n"
                + "|3200|                        true|                         16|\n"
                + "+----+----------------------------+---------------------------+\n"
                + "Total line number = 1\n";
        executeAndCompare(query, expected);
    }

    @Test
    public void testClearData() {
        String clearData = "CLEAR DATA;";
        execute(clearData);

        String countPoints = "COUNT POINTS;";
        String expected = "Points num: 0\n";
        executeAndCompare(countPoints, expected);

        String showTimeSeries = "SELECT * FROM *;";
        expected = "ResultSets:\n" +
            "+----+\n" +
            "|Time|\n" +
            "+----+\n" +
            "+----+\n" +
            "Empty set.\n";
        executeAndCompare(showTimeSeries, expected);
    }

}
