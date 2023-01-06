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

    protected static final Logger logger = LoggerFactory.getLogger(TagIT.class);

    protected static Session session;

    protected String storageEngineType;
    protected static boolean ifClearData;

    private String CLEARDATAEXCP = "cn.edu.tsinghua.iginx.exceptions.ExecutionException: Caution: can not clear the data of read-only node.";

    @BeforeClass
    public static void setUp() {
        ifClearData = true;
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
        String[] insertStatements = (
            "insert into ah.hr01 (key, s, v, s[t1=v1, t2=vv1], v[t1=v2, t2=vv1]) values (0, 1, 2, 3, 4), (1, 2, 3, 4, 5), (2, 3, 4, 5, 6), (3, 4, 5, 6, 7);\n" +
            "insert into ah.hr02 (key, s, v) values (100, true, \"v1\");\n" +
            "insert into ah.hr02[t1=v1] (key, s, v) values (400, false, \"v4\");\n" +
            "insert into ah.hr02[t1=v1,t2=v2] (key, v) values (800, \"v8\");\n" +
            "insert into ah.hr03 (key, s[t1=vv1,t2=v2], v[t1=vv11]) values (1600, true, 16);\n" +
            "insert into ah.hr03 (key, s[t1=v1,t2=vv2], v[t1=v1], v[t1=vv11]) values (3200, true, 16, 32);"
        ).split("\n");

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
        if(!ifClearData) return;

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
            if (e.toString().equals(CLEARDATAEXCP)) {
                logger.error("clear data fail and go on....");
            }
            else fail();
        }

        if (res==null) {
            return "";
        }

        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}.", statement, res.getParseErrorMsg());
            fail();
            return "";
        }

        return res.getResultInString(false, "");
    }

    @Test
    public void capacityExpansion() {
        if(ifClearData) return;

        testShowTimeSeriesWithTags();

        testCountPath();

        testBasicQuery();

        testQueryWithoutTags();

        testQueryWithTag();

        testQueryWithMultiTags();

        testQueryWithWildcardTag();

        testQueryWithAggregate();

        testMixQueryWithAggregate();

        testAlias();

        testSubQuery();

        testTagInsertWithSubQuery();

//        testDeleteWithTag();

        testDeleteWithMultiTags();

        testDeleteTSWithMultiTags();
    }

    @Test
    public void testShowTimeSeriesWithTags() {
        String statement = "SHOW TIME SERIES ah.*;";
        String expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr01.s|    LONG|\n"
                        + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
                        + "|              ah.hr01.v|    LONG|\n"
                        + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|              ah.hr02.v|  BINARY|\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                        + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                        + "|       ah.hr03.v{t1=v1}|    LONG|\n"
                        + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 13\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.* limit 6;";
        expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr01.s|    LONG|\n"
                        + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
                        + "|              ah.hr01.v|    LONG|\n"
                        + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 6\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.* limit 3 offset 7;";
        expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.* limit 7, 3;";
        expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.hr02.*;";
        expected =
                "Time series:\n"
                        + "+----------------------+--------+\n"
                        + "|                  Path|DataType|\n"
                        + "+----------------------+--------+\n"
                        + "|             ah.hr02.s| BOOLEAN|\n"
                        + "|      ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|             ah.hr02.v|  BINARY|\n"
                        + "|      ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "+----------------------+--------+\n"
                        + "Total line number = 5\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.hr02.* limit 3 offset 2;";
        expected =
                "Time series:\n"
                        + "+----------------------+--------+\n"
                        + "|                  Path|DataType|\n"
                        + "+----------------------+--------+\n"
                        + "|             ah.hr02.v|  BINARY|\n"
                        + "|      ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "+----------------------+--------+\n"
                        + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.hr02.*, ah.hr03.*;";
        expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|              ah.hr02.v|  BINARY|\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                        + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                        + "|       ah.hr03.v{t1=v1}|    LONG|\n"
                        + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 9\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.hr02.* with t1=v1;";
        expected =
                "Time series:\n"
                        + "+----------------------+--------+\n"
                        + "|                  Path|DataType|\n"
                        + "+----------------------+--------+\n"
                        + "|      ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|      ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "+----------------------+--------+\n"
                        + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.hr02.* with t1=v1 limit 2 offset 1;";
        expected =
                "Time series:\n"
                        + "+----------------------+--------+\n"
                        + "|                  Path|DataType|\n"
                        + "+----------------------+--------+\n"
                        + "|      ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "+----------------------+--------+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.hr02.* with_precise t1=v1;";
        expected =
                "Time series:\n"
                        + "+----------------+--------+\n"
                        + "|            Path|DataType|\n"
                        + "+----------------+--------+\n"
                        + "|ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "+----------------+--------+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.hr02.* with_precise t1=v1 AND t2=v2;";
        expected =
                "Time series:\n"
                        + "+----------------------+--------+\n"
                        + "|                  Path|DataType|\n"
                        + "+----------------------+--------+\n"
                        + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "+----------------------+--------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES ah.hr02.* with t1=v1 AND t2=v2;";
        expected =
                "Time series:\n"
                        + "+----------------------+--------+\n"
                        + "|                  Path|DataType|\n"
                        + "+----------------------+--------+\n"
                        + "|ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "+----------------------+--------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES  ah.* WITHOUT TAG;";
        expected =
                "Time series:\n"
                        + "+---------+--------+\n"
                        + "|     Path|DataType|\n"
                        + "+---------+--------+\n"
                        + "|ah.hr01.s|    LONG|\n"
                        + "|ah.hr01.v|    LONG|\n"
                        + "|ah.hr02.s| BOOLEAN|\n"
                        + "|ah.hr02.v|  BINARY|\n"
                        + "+---------+--------+\n"
                        + "Total line number = 4\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testCountPoints() {
        String statement = "COUNT POINTS;";
        String expected = "Points num: 26\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testCountPath() {
        String statement = "SELECT COUNT(*) FROM ah;";
        String expected =
                "ResultSets:\n"
                        + "+----------------+------------------------------+----------------+------------------------------+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n"
                        + "|count(ah.hr01.s)|count(ah.hr01.s{t1=v1,t2=vv1})|count(ah.hr01.v)|count(ah.hr01.v{t1=v2,t2=vv1})|count(ah.hr02.s)|count(ah.hr02.s{t1=v1})|count(ah.hr02.v)|count(ah.hr02.v{t1=v1,t2=v2})|count(ah.hr02.v{t1=v1})|count(ah.hr03.s{t1=v1,t2=vv2})|count(ah.hr03.s{t1=vv1,t2=v2})|count(ah.hr03.v{t1=v1})|count(ah.hr03.v{t1=vv11})|\n"
                        + "+----------------+------------------------------+----------------+------------------------------+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n"
                        + "|               4|                             4|               4|                             4|               1|                      1|               1|                            1|                      1|                             1|                             1|                      1|                        2|\n"
                        + "+----------------+------------------------------+----------------+------------------------------+----------------+-----------------------+----------------+-----------------------------+-----------------------+------------------------------+------------------------------+-----------------------+-------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testBasicQuery() {
        String statement = "SELECT * FROM ah;";
        String expected =
                "ResultSets:\n"
                        + "+----+---------+-----------------------+---------+-----------------------+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n"
                        + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr01.v|ah.hr01.v{t1=v2,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr02.v|ah.hr02.v{t1=v1,t2=v2}|ah.hr02.v{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|ah.hr03.v{t1=v1}|ah.hr03.v{t1=vv11}|\n"
                        + "+----+---------+-----------------------+---------+-----------------------+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n"
                        + "|   0|        1|                      3|        2|                      4|     null|            null|     null|                  null|            null|                   null|                   null|            null|              null|\n"
                        + "|   1|        2|                      4|        3|                      5|     null|            null|     null|                  null|            null|                   null|                   null|            null|              null|\n"
                        + "|   2|        3|                      5|        4|                      6|     null|            null|     null|                  null|            null|                   null|                   null|            null|              null|\n"
                        + "|   3|        4|                      6|        5|                      7|     null|            null|     null|                  null|            null|                   null|                   null|            null|              null|\n"
                        + "| 100|     null|                   null|     null|                   null|     true|            null|       v1|                  null|            null|                   null|                   null|            null|              null|\n"
                        + "| 400|     null|                   null|     null|                   null|     null|           false|     null|                  null|              v4|                   null|                   null|            null|              null|\n"
                        + "| 800|     null|                   null|     null|                   null|     null|            null|     null|                    v8|            null|                   null|                   null|            null|              null|\n"
                        + "|1600|     null|                   null|     null|                   null|     null|            null|     null|                  null|            null|                   null|                   true|            null|                16|\n"
                        + "|3200|     null|                   null|     null|                   null|     null|            null|     null|                  null|            null|                   true|                   null|              16|                32|\n"
                        + "+----+---------+-----------------------+---------+-----------------------+---------+----------------+---------+----------------------+----------------+-----------------------+-----------------------+----------------+------------------+\n"
                        + "Total line number = 9\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testQueryWithoutTags() {
        String statement = "SELECT s FROM ah.*;";
        String expected =
                "ResultSets:\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
                        + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
                        + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
                        + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
                        + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
                        + "| 400|     null|                   null|     null|           false|                   null|                   null|\n"
                        + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
                        + "|3200|     null|                   null|     null|            null|                   true|                   null|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "Total line number = 8\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ah.* WITHOUT TAG;";
        expected =
                "ResultSets:\n"
                        + "+---+---------+---------+\n"
                        + "|key|ah.hr01.s|ah.hr02.s|\n"
                        + "+---+---------+---------+\n"
                        + "|  0|        1|     null|\n"
                        + "|  1|        2|     null|\n"
                        + "|  2|        3|     null|\n"
                        + "|  3|        4|     null|\n"
                        + "|100|     null|     true|\n"
                        + "+---+---------+---------+\n"
                        + "Total line number = 5\n";
        executeAndCompare(statement, expected);

        statement = "SELECT v FROM ah.*;";
        expected =
                "ResultSets:\n"
                        + "+----+---------+-----------------------+---------+----------------------+----------------+----------------+------------------+\n"
                        + "| key|ah.hr01.v|ah.hr01.v{t1=v2,t2=vv1}|ah.hr02.v|ah.hr02.v{t1=v1,t2=v2}|ah.hr02.v{t1=v1}|ah.hr03.v{t1=v1}|ah.hr03.v{t1=vv11}|\n"
                        + "+----+---------+-----------------------+---------+----------------------+----------------+----------------+------------------+\n"
                        + "|   0|        2|                      4|     null|                  null|            null|            null|              null|\n"
                        + "|   1|        3|                      5|     null|                  null|            null|            null|              null|\n"
                        + "|   2|        4|                      6|     null|                  null|            null|            null|              null|\n"
                        + "|   3|        5|                      7|     null|                  null|            null|            null|              null|\n"
                        + "| 100|     null|                   null|       v1|                  null|            null|            null|              null|\n"
                        + "| 400|     null|                   null|     null|                  null|              v4|            null|              null|\n"
                        + "| 800|     null|                   null|     null|                    v8|            null|            null|              null|\n"
                        + "|1600|     null|                   null|     null|                  null|            null|            null|                16|\n"
                        + "|3200|     null|                   null|     null|                  null|            null|              16|                32|\n"
                        + "+----+---------+-----------------------+---------+----------------------+----------------+----------------+------------------+\n"
                        + "Total line number = 9\n";
        executeAndCompare(statement, expected);

        statement = "SELECT v FROM ah.* WITHOUT TAG;";
        expected =
                "ResultSets:\n"
                        + "+---+---------+---------+\n"
                        + "|key|ah.hr01.v|ah.hr02.v|\n"
                        + "+---+---------+---------+\n"
                        + "|  0|        2|     null|\n"
                        + "|  1|        3|     null|\n"
                        + "|  2|        4|     null|\n"
                        + "|  3|        5|     null|\n"
                        + "|100|     null|       v1|\n"
                        + "+---+---------+---------+\n"
                        + "Total line number = 5\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testQueryWithTag() {
        String statement = "SELECT s FROM ah.* with t1=v1;";
        String expected =
                "ResultSets:\n"
                        + "+----+-----------------------+----------------+-----------------------+\n"
                        + "| key|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|\n"
                        + "+----+-----------------------+----------------+-----------------------+\n"
                        + "|   0|                      3|            null|                   null|\n"
                        + "|   1|                      4|            null|                   null|\n"
                        + "|   2|                      5|            null|                   null|\n"
                        + "|   3|                      6|            null|                   null|\n"
                        + "| 400|                   null|           false|                   null|\n"
                        + "|3200|                   null|            null|                   true|\n"
                        + "+----+-----------------------+----------------+-----------------------+\n"
                        + "Total line number = 6\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ah.* with_precise t1=v1;";
        expected =
                "ResultSets:\n"
                        + "+---+----------------+\n"
                        + "|key|ah.hr02.s{t1=v1}|\n"
                        + "+---+----------------+\n"
                        + "|400|           false|\n"
                        + "+---+----------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testQueryWithMultiTags() {
        String statement = "SELECT s FROM ah.* with t1=v1 OR t2=v2;";
        String expected =
                "ResultSets:\n"
                        + "+----+-----------------------+----------------+-----------------------+-----------------------+\n"
                        + "| key|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+-----------------------+----------------+-----------------------+-----------------------+\n"
                        + "|   0|                      3|            null|                   null|                   null|\n"
                        + "|   1|                      4|            null|                   null|                   null|\n"
                        + "|   2|                      5|            null|                   null|                   null|\n"
                        + "|   3|                      6|            null|                   null|                   null|\n"
                        + "| 400|                   null|           false|                   null|                   null|\n"
                        + "|1600|                   null|            null|                   null|                   true|\n"
                        + "|3200|                   null|            null|                   true|                   null|\n"
                        + "+----+-----------------------+----------------+-----------------------+-----------------------+\n"
                        + "Total line number = 7\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ah.* with t1=v1 AND t2=vv2;";
        expected = "ResultSets:\n" +
                "+----+-----------------------+\n" +
                "| key|ah.hr03.s{t1=v1,t2=vv2}|\n" +
                "+----+-----------------------+\n" +
                "|3200|                   true|\n" +
                "+----+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ah.* with_precise t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
        expected =
                "ResultSets:\n"
                        + "+----+-----------------------+-----------------------+\n"
                        + "| key|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+-----------------------+-----------------------+\n"
                        + "|1600|                   null|                   true|\n"
                        + "|3200|                   true|                   null|\n"
                        + "+----+-----------------------+-----------------------+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ah.* with_precise t1=v1;";
        expected =
                "ResultSets:\n"
                        + "+---+----------------+\n"
                        + "|key|ah.hr02.s{t1=v1}|\n"
                        + "+---+----------------+\n"
                        + "|400|           false|\n"
                        + "+---+----------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testDeleteWithTag() {
        String statement = "SELECT s FROM ah.*;";
        String expected =
                "ResultSets:\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
                        + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
                        + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
                        + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
                        + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
                        + "| 400|     null|                   null|     null|           false|                   null|                   null|\n"
                        + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
                        + "|3200|     null|                   null|     null|            null|                   true|                   null|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "Total line number = 8\n";
        executeAndCompare(statement, expected);

        statement = "DELETE FROM ah.*.s WHERE key > 10 WITH t1=v1;";
        execute(statement);

        statement = "SELECT s FROM ah.*;";
        expected =
                "ResultSets:\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
                        + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
                        + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
                        + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
                        + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
                        + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "Total line number = 6\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testDeleteWithMultiTags() {
        String statement = "SELECT s FROM ah.*;";
        String expected =
                "ResultSets:\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
                        + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
                        + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
                        + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
                        + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
                        + "| 400|     null|                   null|     null|           false|                   null|                   null|\n"
                        + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
                        + "|3200|     null|                   null|     null|            null|                   true|                   null|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "Total line number = 8\n";
        executeAndCompare(statement, expected);

        statement = "DELETE FROM ah.*.s WHERE key > 10 WITH t1=v1 AND t2=vv2;";
        execute(statement);

        statement = "SELECT s FROM ah.*;";
        expected =
                "ResultSets:\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
                        + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
                        + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
                        + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
                        + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
                        + "| 400|     null|                   null|     null|           false|                   null|                   null|\n"
                        + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "Total line number = 7\n";
        executeAndCompare(statement, expected);

        statement = "DELETE FROM ah.*.s WHERE key > 10 WITH_PRECISE t1=v1;";
        execute(statement);

        statement = "SELECT s FROM ah.*;";
        expected =
                "ResultSets:\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "| key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "|   0|        1|                      3|     null|            null|                   null|                   null|\n"
                        + "|   1|        2|                      4|     null|            null|                   null|                   null|\n"
                        + "|   2|        3|                      5|     null|            null|                   null|                   null|\n"
                        + "|   3|        4|                      6|     null|            null|                   null|                   null|\n"
                        + "| 100|     null|                   null|     true|            null|                   null|                   null|\n"
                        + "|1600|     null|                   null|     null|            null|                   null|                   true|\n"
                        + "+----+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "Total line number = 6\n";
        executeAndCompare(statement, expected);

        statement = "DELETE FROM ah.*.s WHERE key > 10 WITH t1=v1 OR t2=v2;";
        execute(statement);

        statement = "SELECT s FROM ah.*;";
        expected =
                "ResultSets:\n"
                        + "+---+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "|key|ah.hr01.s|ah.hr01.s{t1=v1,t2=vv1}|ah.hr02.s|ah.hr02.s{t1=v1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+---+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "|  0|        1|                      3|     null|            null|                   null|                   null|\n"
                        + "|  1|        2|                      4|     null|            null|                   null|                   null|\n"
                        + "|  2|        3|                      5|     null|            null|                   null|                   null|\n"
                        + "|  3|        4|                      6|     null|            null|                   null|                   null|\n"
                        + "|100|     null|                   null|     true|            null|                   null|                   null|\n"
                        + "+---+---------+-----------------------+---------+----------------+-----------------------+-----------------------+\n"
                        + "Total line number = 5\n";
        executeAndCompare(statement, expected);

    }

    @Test
    public void testDeleteTSWithTag() {
        String showTimeSeries = "SHOW TIME SERIES ah.*;";
        String expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr01.s|    LONG|\n"
                        + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
                        + "|              ah.hr01.v|    LONG|\n"
                        + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|              ah.hr02.v|  BINARY|\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                        + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                        + "|       ah.hr03.v{t1=v1}|    LONG|\n"
                        + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 13\n";
        executeAndCompare(showTimeSeries, expected);

        String deleteTimeSeries = "DELETE TIME SERIES ah.*.s WITH t1=v1";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES ah.*;";
        expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr01.s|    LONG|\n"
                        + "|              ah.hr01.v|    LONG|\n"
                        + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|              ah.hr02.v|  BINARY|\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                        + "|       ah.hr03.v{t1=v1}|    LONG|\n"
                        + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 10\n";
        executeAndCompare(showTimeSeries, expected);

        String showTimeSeriesData = "SELECT s FROM ah.* WITH t1=v1;";
        expected = "ResultSets:\n" +
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";
        executeAndCompare(showTimeSeriesData, expected);

        deleteTimeSeries = "DELETE TIME SERIES ah.*.v WITH_PRECISE t1=v1";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES ah.*;";
        expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr01.s|    LONG|\n"
                        + "|              ah.hr01.v|    LONG|\n"
                        + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|              ah.hr02.v|  BINARY|\n"
                        + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                        + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 8\n";
        executeAndCompare(showTimeSeries, expected);

        showTimeSeriesData = "SELECT v FROM ah.* WITH t1=v1;";
        expected =
                "ResultSets:\n"
                        + "+---+----------------------+\n"
                        + "|key|ah.hr02.v{t1=v1,t2=v2}|\n"
                        + "+---+----------------------+\n"
                        + "|800|                    v8|\n"
                        + "+---+----------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(showTimeSeriesData, expected);
    }

    @Test
    public void testDeleteTSWithMultiTags() {
        String showTimeSeries = "SHOW TIME SERIES ah.*;";
        String expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr01.s|    LONG|\n"
                        + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
                        + "|              ah.hr01.v|    LONG|\n"
                        + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|              ah.hr02.v|  BINARY|\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "| ah.hr02.v{t1=v1,t2=v2}|  BINARY|\n"
                        + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                        + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                        + "|       ah.hr03.v{t1=v1}|    LONG|\n"
                        + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 13\n";
        executeAndCompare(showTimeSeries, expected);

        String deleteTimeSeries = "DELETE TIME SERIES ah.*.v WITH t1=v1 AND t2=v2;";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES ah.*;";
        expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr01.s|    LONG|\n"
                        + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
                        + "|              ah.hr01.v|    LONG|\n"
                        + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|              ah.hr02.v|  BINARY|\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "|ah.hr03.s{t1=v1,t2=vv2}| BOOLEAN|\n"
                        + "|ah.hr03.s{t1=vv1,t2=v2}| BOOLEAN|\n"
                        + "|       ah.hr03.v{t1=v1}|    LONG|\n"
                        + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 12\n";
        executeAndCompare(showTimeSeries, expected);

        String showTimeSeriesData = "SELECT v FROM ah.* WITH t1=v1 AND t2=v2;";
        expected = "ResultSets:\n" +
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";;
        executeAndCompare(showTimeSeriesData, expected);

        deleteTimeSeries = "DELETE TIME SERIES * WITH t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES ah.*;";
        expected =
                "Time series:\n"
                        + "+-----------------------+--------+\n"
                        + "|                   Path|DataType|\n"
                        + "+-----------------------+--------+\n"
                        + "|              ah.hr01.s|    LONG|\n"
                        + "|ah.hr01.s{t1=v1,t2=vv1}|    LONG|\n"
                        + "|              ah.hr01.v|    LONG|\n"
                        + "|ah.hr01.v{t1=v2,t2=vv1}|    LONG|\n"
                        + "|              ah.hr02.s| BOOLEAN|\n"
                        + "|       ah.hr02.s{t1=v1}| BOOLEAN|\n"
                        + "|              ah.hr02.v|  BINARY|\n"
                        + "|       ah.hr02.v{t1=v1}|  BINARY|\n"
                        + "|       ah.hr03.v{t1=v1}|    LONG|\n"
                        + "|     ah.hr03.v{t1=vv11}|    LONG|\n"
                        + "+-----------------------+--------+\n"
                        + "Total line number = 10\n";
        executeAndCompare(showTimeSeries, expected);

        showTimeSeriesData = "SELECT * FROM * WITH t1=v1 AND t2=vv2 OR t1=vv1 AND t2=v2;";
        expected = "ResultSets:\n" +
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";;
        executeAndCompare(showTimeSeriesData, expected);
    }

    @Test
    public void testQueryWithWildcardTag() {
        String statement = "SELECT s FROM ah.* with t2=*;";
        String expected =
                "ResultSets:\n"
                        + "+----+-----------------------+-----------------------+-----------------------+\n"
                        + "| key|ah.hr01.s{t1=v1,t2=vv1}|ah.hr03.s{t1=v1,t2=vv2}|ah.hr03.s{t1=vv1,t2=v2}|\n"
                        + "+----+-----------------------+-----------------------+-----------------------+\n"
                        + "|   0|                      3|                   null|                   null|\n"
                        + "|   1|                      4|                   null|                   null|\n"
                        + "|   2|                      5|                   null|                   null|\n"
                        + "|   3|                      6|                   null|                   null|\n"
                        + "|1600|                   null|                   null|                   true|\n"
                        + "|3200|                   null|                   true|                   null|\n"
                        + "+----+-----------------------+-----------------------+-----------------------+\n"
                        + "Total line number = 6\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testQueryWithAggregate() {
        String statement = "SELECT sum(v) FROM ah.hr03 with t1=vv11;";
        String expected = "ResultSets:\n" +
                "+-----------------------+\n" +
                "|sum(ah.hr03.v{t1=vv11})|\n" +
                "+-----------------------+\n" +
                "|                     48|\n" +
                "+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT max(v) FROM ah.hr03 with t1=vv11;";
        expected = "ResultSets:\n" +
                "+-----------------------+\n" +
                "|max(ah.hr03.v{t1=vv11})|\n" +
                "+-----------------------+\n" +
                "|                     32|\n" +
                "+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT min(v) FROM ah.hr03 with t1=vv11;";
        expected = "ResultSets:\n" +
                "+-----------------------+\n" +
                "|min(ah.hr03.v{t1=vv11})|\n" +
                "+-----------------------+\n" +
                "|                     16|\n" +
                "+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT avg(v) FROM ah.hr03 with t1=vv11;";
        expected = "ResultSets:\n" +
                "+-----------------------+\n" +
                "|avg(ah.hr03.v{t1=vv11})|\n" +
                "+-----------------------+\n" +
                "|                   24.0|\n" +
                "+-----------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT count(v) FROM ah.hr03 with t1=vv11;";
        expected = "ResultSets:\n" +
                "+-------------------------+\n" +
                "|count(ah.hr03.v{t1=vv11})|\n" +
                "+-------------------------+\n" +
                "|                        2|\n" +
                "+-------------------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testMixQueryWithAggregate() {
        String statement = "select last(s) from ah.hr01;";
        String expected =
                "ResultSets:\n"
                        + "+---+-----------------------+-----+\n"
                        + "|key|                   path|value|\n"
                        + "+---+-----------------------+-----+\n"
                        + "|  3|              ah.hr01.s|    4|\n"
                        + "|  3|ah.hr01.s{t1=v1,t2=vv1}|    6|\n"
                        + "+---+-----------------------+-----+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "select last(v) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+---+-----------------------+-----+\n"
                        + "|key|                   path|value|\n"
                        + "+---+-----------------------+-----+\n"
                        + "|  3|              ah.hr01.v|    5|\n"
                        + "|  3|ah.hr01.v{t1=v2,t2=vv1}|    7|\n"
                        + "+---+-----------------------+-----+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "select first(s) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+---+-----------------------+-----+\n"
                        + "|key|                   path|value|\n"
                        + "+---+-----------------------+-----+\n"
                        + "|  0|              ah.hr01.s|    1|\n"
                        + "|  0|ah.hr01.s{t1=v1,t2=vv1}|    3|\n"
                        + "+---+-----------------------+-----+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "select first(v) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+---+-----------------------+-----+\n"
                        + "|key|                   path|value|\n"
                        + "+---+-----------------------+-----+\n"
                        + "|  0|              ah.hr01.v|    2|\n"
                        + "|  0|ah.hr01.v{t1=v2,t2=vv1}|    4|\n"
                        + "+---+-----------------------+-----+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "select first(s), last(v) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+---+-----------------------+-----+\n"
                        + "|key|                   path|value|\n"
                        + "+---+-----------------------+-----+\n"
                        + "|  0|              ah.hr01.s|    1|\n"
                        + "|  0|ah.hr01.s{t1=v1,t2=vv1}|    3|\n"
                        + "|  3|              ah.hr01.v|    5|\n"
                        + "|  3|ah.hr01.v{t1=v2,t2=vv1}|    7|\n"
                        + "+---+-----------------------+-----+\n"
                        + "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "select first(v), last(s) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+---+-----------------------+-----+\n"
                        + "|key|                   path|value|\n"
                        + "+---+-----------------------+-----+\n"
                        + "|  0|              ah.hr01.v|    2|\n"
                        + "|  0|ah.hr01.v{t1=v2,t2=vv1}|    4|\n"
                        + "|  3|              ah.hr01.s|    4|\n"
                        + "|  3|ah.hr01.s{t1=v1,t2=vv1}|    6|\n"
                        + "+---+-----------------------+-----+\n"
                        + "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "select first(v), last(v) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+---+-----------------------+-----+\n"
                        + "|key|                   path|value|\n"
                        + "+---+-----------------------+-----+\n"
                        + "|  0|              ah.hr01.v|    2|\n"
                        + "|  0|ah.hr01.v{t1=v2,t2=vv1}|    4|\n"
                        + "|  3|              ah.hr01.v|    5|\n"
                        + "|  3|ah.hr01.v{t1=v2,t2=vv1}|    7|\n"
                        + "+---+-----------------------+-----+\n"
                        + "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "select first(s), last(s) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+---+-----------------------+-----+\n"
                        + "|key|                   path|value|\n"
                        + "+---+-----------------------+-----+\n"
                        + "|  0|              ah.hr01.s|    1|\n"
                        + "|  0|ah.hr01.s{t1=v1,t2=vv1}|    3|\n"
                        + "|  3|              ah.hr01.s|    4|\n"
                        + "|  3|ah.hr01.s{t1=v1,t2=vv1}|    6|\n"
                        + "+---+-----------------------+-----+\n"
                        + "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "select first_value(*) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+----------------------+------------------------------------+----------------------+------------------------------------+\n"
                        + "|first_value(ah.hr01.s)|first_value(ah.hr01.s{t1=v1,t2=vv1})|first_value(ah.hr01.v)|first_value(ah.hr01.v{t1=v2,t2=vv1})|\n"
                        + "+----------------------+------------------------------------+----------------------+------------------------------------+\n"
                        + "|                     1|                                   3|                     2|                                   4|\n"
                        + "+----------------------+------------------------------------+----------------------+------------------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "select last_value(*) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+---------------------+-----------------------------------+---------------------+-----------------------------------+\n"
                        + "|last_value(ah.hr01.s)|last_value(ah.hr01.s{t1=v1,t2=vv1})|last_value(ah.hr01.v)|last_value(ah.hr01.v{t1=v2,t2=vv1})|\n"
                        + "+---------------------+-----------------------------------+---------------------+-----------------------------------+\n"
                        + "|                    4|                                  6|                    5|                                  7|\n"
                        + "+---------------------+-----------------------------------+---------------------+-----------------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "select max(*) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "|max(ah.hr01.s)|max(ah.hr01.s{t1=v1,t2=vv1})|max(ah.hr01.v)|max(ah.hr01.v{t1=v2,t2=vv1})|\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "|             4|                           6|             5|                           7|\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "select min(*) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "|min(ah.hr01.s)|min(ah.hr01.s{t1=v1,t2=vv1})|min(ah.hr01.v)|min(ah.hr01.v{t1=v2,t2=vv1})|\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "|             1|                           3|             2|                           4|\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "select sum(*) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "|sum(ah.hr01.s)|sum(ah.hr01.s{t1=v1,t2=vv1})|sum(ah.hr01.v)|sum(ah.hr01.v{t1=v2,t2=vv1})|\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "|            10|                          18|            14|                          22|\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "select count(*) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+----------------+------------------------------+----------------+------------------------------+\n"
                        + "|count(ah.hr01.s)|count(ah.hr01.s{t1=v1,t2=vv1})|count(ah.hr01.v)|count(ah.hr01.v{t1=v2,t2=vv1})|\n"
                        + "+----------------+------------------------------+----------------+------------------------------+\n"
                        + "|               4|                             4|               4|                             4|\n"
                        + "+----------------+------------------------------+----------------+------------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "select avg(*) from ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "|avg(ah.hr01.s)|avg(ah.hr01.s{t1=v1,t2=vv1})|avg(ah.hr01.v)|avg(ah.hr01.v{t1=v2,t2=vv1})|\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "|           2.5|                         4.5|           3.5|                         5.5|\n"
                        + "+--------------+----------------------------+--------------+----------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT first(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+----+------------------+-----+\n"
                        + "| key|              path|value|\n"
                        + "+----+------------------+-----+\n"
                        + "|1600|ah.hr03.v{t1=vv11}|   16|\n"
                        + "|3200|  ah.hr03.v{t1=v1}|   16|\n"
                        + "+----+------------------+-----+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT last(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+----+------------------+-----+\n"
                        + "| key|              path|value|\n"
                        + "+----+------------------+-----+\n"
                        + "|3200|ah.hr03.v{t1=vv11}|   32|\n"
                        + "|3200|  ah.hr03.v{t1=v1}|   16|\n"
                        + "+----+------------------+-----+\n"
                        + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT first_value(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+-----------------------------+-------------------------------+\n"
                        + "|first_value(ah.hr03.v{t1=v1})|first_value(ah.hr03.v{t1=vv11})|\n"
                        + "+-----------------------------+-------------------------------+\n"
                        + "|                           16|                             16|\n"
                        + "+-----------------------------+-------------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT last_value(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+----------------------------+------------------------------+\n"
                        + "|last_value(ah.hr03.v{t1=v1})|last_value(ah.hr03.v{t1=vv11})|\n"
                        + "+----------------------------+------------------------------+\n"
                        + "|                          16|                            32|\n"
                        + "+----------------------------+------------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT max(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+---------------------+-----------------------+\n"
                        + "|max(ah.hr03.v{t1=v1})|max(ah.hr03.v{t1=vv11})|\n"
                        + "+---------------------+-----------------------+\n"
                        + "|                   16|                     32|\n"
                        + "+---------------------+-----------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT min(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+---------------------+-----------------------+\n"
                        + "|min(ah.hr03.v{t1=v1})|min(ah.hr03.v{t1=vv11})|\n"
                        + "+---------------------+-----------------------+\n"
                        + "|                   16|                     16|\n"
                        + "+---------------------+-----------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT sum(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+---------------------+-----------------------+\n"
                        + "|sum(ah.hr03.v{t1=v1})|sum(ah.hr03.v{t1=vv11})|\n"
                        + "+---------------------+-----------------------+\n"
                        + "|                   16|                     48|\n"
                        + "+---------------------+-----------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT count(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+-----------------------+-------------------------+\n"
                        + "|count(ah.hr03.v{t1=v1})|count(ah.hr03.v{t1=vv11})|\n"
                        + "+-----------------------+-------------------------+\n"
                        + "|                      1|                        2|\n"
                        + "+-----------------------+-------------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT avg(v) FROM ah.hr03;";
        expected =
                "ResultSets:\n"
                        + "+---------------------+-----------------------+\n"
                        + "|avg(ah.hr03.v{t1=v1})|avg(ah.hr03.v{t1=vv11})|\n"
                        + "+---------------------+-----------------------+\n"
                        + "|                 16.0|                   24.0|\n"
                        + "+---------------------+-----------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testAlias() {
        String statement = "SELECT s AS ts FROM ah.hr02;";
        String expected = "ResultSets:\n" +
                "+---+----+---------+\n" +
                "|key|  ts|ts{t1=v1}|\n" +
                "+---+----+---------+\n" +
                "|100|true|     null|\n" +
                "|400|null|    false|\n" +
                "+---+----+---------+\n" +
                "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s FROM ah.hr02 AS result_set;";
        expected = "ResultSets:\n" +
                "+---+--------------------+---------------------------+\n" +
                "|key|result_set.ah.hr02.s|result_set.ah.hr02.s{t1=v1}|\n" +
                "+---+--------------------+---------------------------+\n" +
                "|100|                true|                       null|\n" +
                "|400|                null|                      false|\n" +
                "+---+--------------------+---------------------------+\n" +
                "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s AS ts FROM ah.hr02 AS result_set;";
        expected = "ResultSets:\n" +
                "+---+-------------+--------------------+\n" +
                "|key|result_set.ts|result_set.ts{t1=v1}|\n" +
                "+---+-------------+--------------------+\n" +
                "|100|         true|                null|\n" +
                "|400|         null|               false|\n" +
                "+---+-------------+--------------------+\n" +
                "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testSubQuery() {
        String statement = "SELECT SUM(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
        String expected = "ResultSets:\n" +
                "+---------------+\n" +
                "|sum(ts2{t1=v1})|\n" +
                "+---------------+\n" +
                "|             16|\n" +
                "+---------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT AVG(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
        expected = "ResultSets:\n" +
                "+---------------+\n" +
                "|avg(ts2{t1=v1})|\n" +
                "+---------------+\n" +
                "|           16.0|\n" +
                "+---------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT MAX(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
        expected = "ResultSets:\n" +
                "+---------------+\n" +
                "|max(ts2{t1=v1})|\n" +
                "+---------------+\n" +
                "|             16|\n" +
                "+---------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT COUNT(ts2) FROM (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
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
        String query = "SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1;";
        String expected = "ResultSets:\n" +
                "+----+-----------------+----------+\n" +
                "| key|ts1{t1=v1,t2=vv2}|ts2{t1=v1}|\n" +
                "+----+-----------------+----------+\n" +
                "|3200|             true|        16|\n" +
                "+----+-----------------+----------+\n" +
                "Total line number = 1\n";
        executeAndCompare(query, expected);

        String insert = "INSERT INTO copy.ah.hr01(key, s, v) VALUES (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
        execute(insert);

        query = "SELECT s, v FROM copy.ah.hr01;";
        expected =
                "ResultSets:\n"
                        + "+----+----------------------------+---------------------+\n"
                        + "| key|copy.ah.hr01.s{t1=v1,t2=vv2}|copy.ah.hr01.v{t1=v1}|\n"
                        + "+----+----------------------------+---------------------+\n"
                        + "|3200|                        true|                   16|\n"
                        + "+----+----------------------------+---------------------+\n"
                        + "Total line number = 1\n";
        executeAndCompare(query, expected);

        insert = "INSERT INTO copy.ah.hr02(key, s, v[t2=v2]) VALUES (SELECT s AS ts1, v AS ts2 FROM ah.hr03 with t1=v1);";
        execute(insert);

        query = "SELECT s, v FROM copy.ah.hr02;";
        expected =
                "ResultSets:\n"
                        + "+----+----------------------------+---------------------------+\n"
                        + "| key|copy.ah.hr02.s{t1=v1,t2=vv2}|copy.ah.hr02.v{t1=v1,t2=v2}|\n"
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
                "+---+\n" +
                "|key|\n" +
                "+---+\n" +
                "+---+\n" +
                "Empty set.\n";
        executeAndCompare(showTimeSeries, expected);
    }

}
