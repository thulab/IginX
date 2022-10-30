package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import com.sun.org.apache.xpath.internal.operations.Mult;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class SQLSessionIT {

    protected static MultiConnection session;
    protected static boolean isForSession = true, isForSessionPool = false;
    protected static int MaxMultiThreadTaskNum = -1;

    //host info
    protected static String defaultTestHost = "127.0.0.1";
    protected static int defaultTestPort = 6888;
    protected static String defaultTestUser = "root";
    protected static String defaultTestPass = "root";

    protected static final Logger logger = LoggerFactory.getLogger(SQLSessionIT.class);

    protected boolean isAbleToDelete;

    protected boolean isSupportSpecialPath;

    protected boolean isAbleToShowTimeSeries;

    private final long startTimestamp = 0L;

    private final long endTimestamp = 15000L;

    protected boolean ifClearData = true;

    protected String storageEngineType;

    @BeforeClass
    public static void setUp() {
        if(isForSession)
            session = new MultiConnection (new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
        else if(isForSessionPool)
            session = new MultiConnection ( new SessionPool.Builder()
                    .host(defaultTestHost)
                    .port(defaultTestPort)
                    .user(defaultTestUser)
                    .password(defaultTestPass)
                    .maxSize(MaxMultiThreadTaskNum)
                    .build());
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
        String insertStrPrefix = "INSERT INTO us.d1 (timestamp, s1, s2, s3, s4) values ";

        StringBuilder builder = new StringBuilder(insertStrPrefix);

        int size = (int) (endTimestamp - startTimestamp);
        for (int i = 0; i < size; i++) {
            builder.append(", ");
            builder.append("(");
            builder.append(startTimestamp + i).append(", ");
            builder.append(i).append(", ");
            builder.append(i + 1).append(", ");
            builder.append("\"").append(new String(RandomStringUtils.randomAlphanumeric(10).getBytes())).append("\", ");
            builder.append((i + 0.1));
            builder.append(")");
        }
        builder.append(";");

        String insertStatement = builder.toString();

        SessionExecuteSqlResult res = session.executeSql(insertStatement);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Insert date execute fail. Caused by: {}.", res.getParseErrorMsg());
            fail();
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
        if (!statement.toLowerCase().startsWith("insert")) {
            logger.info("Execute Statement: \"{}\"", statement);
        }

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

    private void executeAndCompareErrMsg(String statement, String expectedErrMsg) {
        logger.info("Execute Statement: \"{}\"", statement);

        try {
            session.executeSql(statement);
        } catch (SessionException | ExecutionException e) {
            logger.info("Statement: \"{}\" execute fail. Because: {}", statement, e.getMessage());
            assertEquals(expectedErrMsg, e.getMessage());
        }
    }

    @Test
    public void capacityExpansion() {
        if (ifClearData) return;

        testCountPath();

        testShowReplicaNum();

        testTimeRangeQuery();

//        testValueFilter();

        testPathFilter();

        testOrderByQuery();

        testFirstLastQuery();

        testAggregateQuery();

        testDownSampleQuery();

        testRangeDownSampleQuery();

//        testFromMultiPath();

        testAlias();

        testAggregateSubQuery();

        testValueFilterSubQuery();

        testMultiSubQuery();

        testDateFormat();

        testSpecialPath();

        testErrorClause();

        testDelete();

        testMultiRangeDelete();

        testCrossRangeDelete();
    }

    @Test
    public void testCountPath() {
        String statement = "SELECT COUNT(*) FROM us.d1;";
        String expected = "ResultSets:\n" +
            "+---------------+---------------+---------------+---------------+\n" +
            "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n" +
            "+---------------+---------------+---------------+---------------+\n" +
            "|          15000|          15000|          15000|          15000|\n" +
            "+---------------+---------------+---------------+---------------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testCountPoints() {
        String statement = "COUNT POINTS;";
        String expected = "Points num: 60000\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testShowTimeSeries() {
        if (!isAbleToShowTimeSeries) {
            return;
        }
        String statement = "SHOW TIME SERIES;";
        String expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s1|    LONG|\n"
                + "|us.d1.s2|    LONG|\n"
                + "|us.d1.s3|  BINARY|\n"
                + "|us.d1.s4|  DOUBLE|\n"
                + "+--------+--------+\n"
                + "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES us.d1.*;";
        expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s1|    LONG|\n"
                + "|us.d1.s2|    LONG|\n"
                + "|us.d1.s3|  BINARY|\n"
                + "|us.d1.s4|  DOUBLE|\n"
                + "+--------+--------+\n"
                + "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES limit 3;";
        expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s1|    LONG|\n"
                + "|us.d1.s2|    LONG|\n"
                + "|us.d1.s3|  BINARY|\n"
                + "+--------+--------+\n"
                + "Total line number = 3\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES limit 2 offset 1;";
        expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s2|    LONG|\n"
                + "|us.d1.s3|  BINARY|\n"
                + "+--------+--------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES limit 1, 2;";
        expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s2|    LONG|\n"
                + "|us.d1.s3|  BINARY|\n"
                + "+--------+--------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES us.d1.s1;";
        expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s1|    LONG|\n"
                + "+--------+--------+\n"
                + "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SHOW TIME SERIES us.d1.s1, us.d1.s3;";
        expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s1|    LONG|\n"
                + "|us.d1.s3|  BINARY|\n"
                + "+--------+--------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testShowReplicaNum() {
        String statement = "SHOW REPLICA NUMBER;";
        String expected = "Replica num: 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testTimeRangeQuery() {
        String statement = "SELECT s1 FROM us.d1 WHERE time > 100 AND time < 120;";
        String expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d1.s1|\n" +
            "+----+--------+\n" +
            "| 101|     101|\n" +
            "| 102|     102|\n" +
            "| 103|     103|\n" +
            "| 104|     104|\n" +
            "| 105|     105|\n" +
            "| 106|     106|\n" +
            "| 107|     107|\n" +
            "| 108|     108|\n" +
            "| 109|     109|\n" +
            "| 110|     110|\n" +
            "| 111|     111|\n" +
            "| 112|     112|\n" +
            "| 113|     113|\n" +
            "| 114|     114|\n" +
            "| 115|     115|\n" +
            "| 116|     116|\n" +
            "| 117|     117|\n" +
            "| 118|     118|\n" +
            "| 119|     119|\n" +
            "+----+--------+\n" +
            "Total line number = 19\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testValueFilter() {
        String query = "SELECT s1 FROM us.d1 WHERE time > 0 AND time < 10000 and s1 > 200 and s1 < 210;";
        String expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d1.s1|\n" +
            "+----+--------+\n" +
            "| 201|     201|\n" +
            "| 202|     202|\n" +
            "| 203|     203|\n" +
            "| 204|     204|\n" +
            "| 205|     205|\n" +
            "| 206|     206|\n" +
            "| 207|     207|\n" +
            "| 208|     208|\n" +
            "| 209|     209|\n" +
            "+----+--------+\n" +
            "Total line number = 9\n";
        executeAndCompare(query, expected);

        String insert = "INSERT INTO us.d2(time, c) VALUES (1, \"asdas\"), (2, \"sadaa\"), (3, \"sadada\"), (4, \"asdad\"), (5, \"deadsa\"), (6, \"dasda\"), (7, \"asdsad\"), (8, \"frgsa\"), (9, \"asdad\");";
        execute(insert);

        query = "SELECT c FROM us.d2 WHERE c like \"^a.*\";";
        expected = "ResultSets:\n" +
            "+----+-------+\n" +
            "|Time|us.d2.c|\n" +
            "+----+-------+\n" +
            "|   1|  asdas|\n" +
            "|   4|  asdad|\n" +
            "|   7| asdsad|\n" +
            "|   9|  asdad|\n" +
            "+----+-------+\n" +
            "Total line number = 4\n";
        executeAndCompare(query, expected);

        query = "SELECT c FROM us.d2 WHERE c like \"^[s|f].*\"";
        expected = "ResultSets:\n" +
            "+----+-------+\n" +
            "|Time|us.d2.c|\n" +
            "+----+-------+\n" +
            "|   2|  sadaa|\n" +
            "|   3| sadada|\n" +
            "|   8|  frgsa|\n" +
            "+----+-------+\n" +
            "Total line number = 3\n";
        executeAndCompare(query, expected);

        query = "SELECT c FROM us.d2 WHERE c like \"^.*[s|d]\";";
        expected = "ResultSets:\n" +
            "+----+-------+\n" +
            "|Time|us.d2.c|\n" +
            "+----+-------+\n" +
            "|   1|  asdas|\n" +
            "|   4|  asdad|\n" +
            "|   7| asdsad|\n" +
            "|   9|  asdad|\n" +
            "+----+-------+\n" +
            "Total line number = 4\n";
        executeAndCompare(query, expected);

        StringBuilder builder = new StringBuilder();
        builder.append("INSERT INTO us.d2(time, s1) VALUES ");
        int size = (int) (endTimestamp - startTimestamp);
        for (int i = 0; i < size; i++) {
            builder.append(", (");
            builder.append(startTimestamp + i).append(", ");
            builder.append(i + 5);
            builder.append(")");
        }
        builder.append(";");

        insert = builder.toString();
        execute(insert);

        query = "SELECT s1 FROM us.* WHERE s1 > 200 and s1 < 210;";
        expected =
            "ResultSets:\n"
                + "+----+--------+--------+\n"
                + "|Time|us.d1.s1|us.d2.s1|\n"
                + "+----+--------+--------+\n"
                + "| 201|     201|     206|\n"
                + "| 202|     202|     207|\n"
                + "| 203|     203|     208|\n"
                + "| 204|     204|     209|\n"
                + "+----+--------+--------+\n"
                + "Total line number = 4\n";
        executeAndCompare(query, expected);
    }

    @Test
    public void testPathFilter() {
        String insert = "INSERT INTO us.d9(time, a, b) VALUES (1, 1, 9), (2, 2, 8), (3, 3, 7), (4, 4, 6), (5, 5, 5), (6, 6, 4), (7, 7, 3), (8, 8, 2), (9, 9, 1);";
        execute(insert);

        String query = "SELECT a, b FROM us.d9 WHERE a > b;";
        String expected = "ResultSets:\n" +
            "+----+-------+-------+\n" +
            "|Time|us.d9.a|us.d9.b|\n" +
            "+----+-------+-------+\n" +
            "|   6|      6|      4|\n" +
            "|   7|      7|      3|\n" +
            "|   8|      8|      2|\n" +
            "|   9|      9|      1|\n" +
            "+----+-------+-------+\n" +
            "Total line number = 4\n";
        executeAndCompare(query, expected);

        query = "SELECT a, b FROM us.d9 WHERE a >= b;";
        expected = "ResultSets:\n" +
            "+----+-------+-------+\n" +
            "|Time|us.d9.a|us.d9.b|\n" +
            "+----+-------+-------+\n" +
            "|   5|      5|      5|\n" +
            "|   6|      6|      4|\n" +
            "|   7|      7|      3|\n" +
            "|   8|      8|      2|\n" +
            "|   9|      9|      1|\n" +
            "+----+-------+-------+\n" +
            "Total line number = 5\n";
        executeAndCompare(query, expected);

        query = "SELECT a, b FROM us.d9 WHERE a < b;";
        expected = "ResultSets:\n" +
            "+----+-------+-------+\n" +
            "|Time|us.d9.a|us.d9.b|\n" +
            "+----+-------+-------+\n" +
            "|   1|      1|      9|\n" +
            "|   2|      2|      8|\n" +
            "|   3|      3|      7|\n" +
            "|   4|      4|      6|\n" +
            "+----+-------+-------+\n" +
            "Total line number = 4\n";
        executeAndCompare(query, expected);

        query = "SELECT a, b FROM us.d9 WHERE a <= b;";
        expected = "ResultSets:\n" +
            "+----+-------+-------+\n" +
            "|Time|us.d9.a|us.d9.b|\n" +
            "+----+-------+-------+\n" +
            "|   1|      1|      9|\n" +
            "|   2|      2|      8|\n" +
            "|   3|      3|      7|\n" +
            "|   4|      4|      6|\n" +
            "|   5|      5|      5|\n" +
            "+----+-------+-------+\n" +
            "Total line number = 5\n";
        executeAndCompare(query, expected);

        query = "SELECT a, b FROM us.d9 WHERE a = b;";
        expected = "ResultSets:\n" +
            "+----+-------+-------+\n" +
            "|Time|us.d9.a|us.d9.b|\n" +
            "+----+-------+-------+\n" +
            "|   5|      5|      5|\n" +
            "+----+-------+-------+\n" +
            "Total line number = 1\n";
        executeAndCompare(query, expected);

        query = "SELECT a, b FROM us.d9 WHERE a != b;";
        expected = "ResultSets:\n" +
            "+----+-------+-------+\n" +
            "|Time|us.d9.a|us.d9.b|\n" +
            "+----+-------+-------+\n" +
            "|   1|      1|      9|\n" +
            "|   2|      2|      8|\n" +
            "|   3|      3|      7|\n" +
            "|   4|      4|      6|\n" +
            "|   6|      6|      4|\n" +
            "|   7|      7|      3|\n" +
            "|   8|      8|      2|\n" +
            "|   9|      9|      1|\n" +
            "+----+-------+-------+\n" +
            "Total line number = 8\n";
        executeAndCompare(query, expected);
    }

    @Test
    public void testLimitAndOffsetQuery() {
        String statement = "SELECT s1 FROM us.d1 WHERE time > 0 AND time < 10000 limit 10;";
        String expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d1.s1|\n" +
            "+----+--------+\n" +
            "|   1|       1|\n" +
            "|   2|       2|\n" +
            "|   3|       3|\n" +
            "|   4|       4|\n" +
            "|   5|       5|\n" +
            "|   6|       6|\n" +
            "|   7|       7|\n" +
            "|   8|       8|\n" +
            "|   9|       9|\n" +
            "|  10|      10|\n" +
            "+----+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);

        statement = "SELECT s1 FROM us.d1 WHERE time > 0 AND time < 10000 limit 10 offset 5;";
        expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d1.s1|\n" +
            "+----+--------+\n" +
            "|   6|       6|\n" +
            "|   7|       7|\n" +
            "|   8|       8|\n" +
            "|   9|       9|\n" +
            "|  10|      10|\n" +
            "|  11|      11|\n" +
            "|  12|      12|\n" +
            "|  13|      13|\n" +
            "|  14|      14|\n" +
            "|  15|      15|\n" +
            "+----+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testOrderByQuery() {
        String insert = "INSERT INTO us.d2 (timestamp, s1, s2, s3) values " +
            "(1, \"apple\", 871, 232.1), (2, \"peach\", 123, 132.5), (3, \"banana\", 356, 317.8);";
        execute(insert);

        String orderByQuery = "SELECT * FROM us.d2 ORDER BY TIME";
        String expected = "ResultSets:\n" +
            "+----+--------+--------+--------+\n" +
            "|Time|us.d2.s1|us.d2.s2|us.d2.s3|\n" +
            "+----+--------+--------+--------+\n" +
            "|   1|   apple|     871|   232.1|\n" +
            "|   2|   peach|     123|   132.5|\n" +
            "|   3|  banana|     356|   317.8|\n" +
            "+----+--------+--------+--------+\n" +
            "Total line number = 3\n";
        executeAndCompare(orderByQuery, expected);
    }

    @Test
    public void testFirstLastQuery() {
        String statement = "SELECT FIRST(s2) FROM us.d1 WHERE time > 0;";
        String expected = "ResultSets:\n" +
            "+----+--------+-----+\n" +
            "|Time|    path|value|\n" +
            "+----+--------+-----+\n" +
            "|   1|us.d1.s2|    2|\n" +
            "+----+--------+-----+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT LAST(s2) FROM us.d1 WHERE time > 0;";
        expected = "ResultSets:\n" +
            "+-----+--------+-----+\n" +
            "| Time|    path|value|\n" +
            "+-----+--------+-----+\n" +
            "|14999|us.d1.s2|15000|\n" +
            "+-----+--------+-----+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT FIRST(s4) FROM us.d1 WHERE time > 0;";
        expected = "ResultSets:\n" +
            "+----+--------+-----+\n" +
            "|Time|    path|value|\n" +
            "+----+--------+-----+\n" +
            "|   1|us.d1.s4|  1.1|\n" +
            "+----+--------+-----+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT LAST(s4) FROM us.d1 WHERE time > 0;";
        expected = "ResultSets:\n" +
            "+-----+--------+-------+\n" +
            "| Time|    path|  value|\n" +
            "+-----+--------+-------+\n" +
            "|14999|us.d1.s4|14999.1|\n" +
            "+-----+--------+-------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);

        statement = "SELECT LAST(s2), LAST(s4) FROM us.d1 WHERE time > 0;";
        expected = "ResultSets:\n" +
            "+-----+--------+-------+\n" +
            "| Time|    path|  value|\n" +
            "+-----+--------+-------+\n" +
            "|14999|us.d1.s2|  15000|\n" +
            "|14999|us.d1.s4|14999.1|\n" +
            "+-----+--------+-------+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT FIRST(s2), FIRST(s4) FROM us.d1 WHERE time > 0;";
        expected = "ResultSets:\n" +
            "+----+--------+-----+\n" +
            "|Time|    path|value|\n" +
            "+----+--------+-----+\n" +
            "|   1|us.d1.s2|    2|\n" +
            "|   1|us.d1.s4|  1.1|\n" +
            "+----+--------+-----+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT LAST(s2), LAST(s4) FROM us.d1 WHERE time < 1000;";
        expected = "ResultSets:\n" +
            "+----+--------+-----+\n" +
            "|Time|    path|value|\n" +
            "+----+--------+-----+\n" +
            "| 999|us.d1.s2| 1000|\n" +
            "| 999|us.d1.s4|999.1|\n" +
            "+----+--------+-----+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT FIRST(s2), LAST(s4) FROM us.d1 WHERE time > 1000;";
        expected = "ResultSets:\n" +
            "+-----+--------+-------+\n" +
            "| Time|    path|  value|\n" +
            "+-----+--------+-------+\n" +
            "| 1001|us.d1.s2|   1002|\n" +
            "|14999|us.d1.s4|14999.1|\n" +
            "+-----+--------+-------+\n" +
            "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT FIRST(s4), LAST(s2) FROM us.d1 WHERE time > 1000;";
        expected =
            "ResultSets:\n"
                + "+-----+--------+------+\n"
                + "| Time|    path| value|\n"
                + "+-----+--------+------+\n"
                + "| 1001|us.d1.s4|1001.1|\n"
                + "|14999|us.d1.s2| 15000|\n"
                + "+-----+--------+------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT FIRST(s2), LAST(s2) FROM us.d1 WHERE time > 1000;";
        expected =
            "ResultSets:\n"
                + "+-----+--------+-----+\n"
                + "| Time|    path|value|\n"
                + "+-----+--------+-----+\n"
                + "| 1001|us.d1.s2| 1002|\n"
                + "|14999|us.d1.s2|15000|\n"
                + "+-----+--------+-----+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);

        statement = "SELECT FIRST(s4), LAST(s4) FROM us.d1 WHERE time > 1000;";
        expected =
            "ResultSets:\n"
                + "+-----+--------+-------+\n"
                + "| Time|    path|  value|\n"
                + "+-----+--------+-------+\n"
                + "| 1001|us.d1.s4| 1001.1|\n"
                + "|14999|us.d1.s4|14999.1|\n"
                + "+-----+--------+-------+\n"
                + "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testAggregateQuery() {
        String statement = "SELECT %s(s1), %s(s2) FROM us.d1 WHERE time > 0 AND time < 1000;";
        List<String> funcTypeList = Arrays.asList(
            "MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT"
        );
        List<String> expectedList = Arrays.asList(
            "ResultSets:\n" +
                "+-------------+-------------+\n" +
                "|max(us.d1.s1)|max(us.d1.s2)|\n" +
                "+-------------+-------------+\n" +
                "|          999|         1000|\n" +
                "+-------------+-------------+\n" +
                "Total line number = 1\n",
            "ResultSets:\n" +
                "+-------------+-------------+\n" +
                "|min(us.d1.s1)|min(us.d1.s2)|\n" +
                "+-------------+-------------+\n" +
                "|            1|            2|\n" +
                "+-------------+-------------+\n" +
                "Total line number = 1\n",
            "ResultSets:\n" +
                "+---------------------+---------------------+\n" +
                "|first_value(us.d1.s1)|first_value(us.d1.s2)|\n" +
                "+---------------------+---------------------+\n" +
                "|                    1|                    2|\n" +
                "+---------------------+---------------------+\n" +
                "Total line number = 1\n",
            "ResultSets:\n" +
                "+--------------------+--------------------+\n" +
                "|last_value(us.d1.s1)|last_value(us.d1.s2)|\n" +
                "+--------------------+--------------------+\n" +
                "|                 999|                1000|\n" +
                "+--------------------+--------------------+\n" +
                "Total line number = 1\n",
            "ResultSets:\n" +
                "+-------------+-------------+\n" +
                "|sum(us.d1.s1)|sum(us.d1.s2)|\n" +
                "+-------------+-------------+\n" +
                "|       499500|       500499|\n" +
                "+-------------+-------------+\n" +
                "Total line number = 1\n",
            "ResultSets:\n" +
                "+-------------+-------------+\n" +
                "|avg(us.d1.s1)|avg(us.d1.s2)|\n" +
                "+-------------+-------------+\n" +
                "|        500.0|        501.0|\n" +
                "+-------------+-------------+\n" +
                "Total line number = 1\n",
            "ResultSets:\n" +
                "+---------------+---------------+\n" +
                "|count(us.d1.s1)|count(us.d1.s2)|\n" +
                "+---------------+---------------+\n" +
                "|            999|            999|\n" +
                "+---------------+---------------+\n" +
                "Total line number = 1\n"
        );
        for (int i = 0; i < funcTypeList.size(); i++) {
            String type = funcTypeList.get(i);
            String expected = expectedList.get(i);
            executeAndCompare(String.format(statement, type, type), expected);
        }
    }

    @Test
    public void testDownSampleQuery() {
        String statement = "SELECT %s(s1), %s(s4) FROM us.d1 GROUP (0, 1000) BY 100ns;";
        List<String> funcTypeList = Arrays.asList(
            "MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT"
        );
        List<String> expectedList = Arrays.asList(
            "ResultSets:\n" +
                "+----+-------------+-------------+\n" +
                "|Time|max(us.d1.s1)|max(us.d1.s4)|\n" +
                "+----+-------------+-------------+\n" +
                "|   1|          100|        100.1|\n" +
                "| 101|          200|        200.1|\n" +
                "| 201|          300|        300.1|\n" +
                "| 301|          400|        400.1|\n" +
                "| 401|          500|        500.1|\n" +
                "| 501|          600|        600.1|\n" +
                "| 601|          700|        700.1|\n" +
                "| 701|          800|        800.1|\n" +
                "| 801|          900|        900.1|\n" +
                "| 901|          999|        999.1|\n" +
                "+----+-------------+-------------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+-------------+-------------+\n" +
                "|Time|min(us.d1.s1)|min(us.d1.s4)|\n" +
                "+----+-------------+-------------+\n" +
                "|   1|            1|          1.1|\n" +
                "| 101|          101|        101.1|\n" +
                "| 201|          201|        201.1|\n" +
                "| 301|          301|        301.1|\n" +
                "| 401|          401|        401.1|\n" +
                "| 501|          501|        501.1|\n" +
                "| 601|          601|        601.1|\n" +
                "| 701|          701|        701.1|\n" +
                "| 801|          801|        801.1|\n" +
                "| 901|          901|        901.1|\n" +
                "+----+-------------+-------------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+---------------------+---------------------+\n" +
                "|Time|first_value(us.d1.s1)|first_value(us.d1.s4)|\n" +
                "+----+---------------------+---------------------+\n" +
                "|   1|                    1|                  1.1|\n" +
                "| 101|                  101|                101.1|\n" +
                "| 201|                  201|                201.1|\n" +
                "| 301|                  301|                301.1|\n" +
                "| 401|                  401|                401.1|\n" +
                "| 501|                  501|                501.1|\n" +
                "| 601|                  601|                601.1|\n" +
                "| 701|                  701|                701.1|\n" +
                "| 801|                  801|                801.1|\n" +
                "| 901|                  901|                901.1|\n" +
                "+----+---------------------+---------------------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+--------------------+--------------------+\n" +
                "|Time|last_value(us.d1.s1)|last_value(us.d1.s4)|\n" +
                "+----+--------------------+--------------------+\n" +
                "|   1|                 100|               100.1|\n" +
                "| 101|                 200|               200.1|\n" +
                "| 201|                 300|               300.1|\n" +
                "| 301|                 400|               400.1|\n" +
                "| 401|                 500|               500.1|\n" +
                "| 501|                 600|               600.1|\n" +
                "| 601|                 700|               700.1|\n" +
                "| 701|                 800|               800.1|\n" +
                "| 801|                 900|               900.1|\n" +
                "| 901|                 999|               999.1|\n" +
                "+----+--------------------+--------------------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+-------------+------------------+\n" +
                "|Time|sum(us.d1.s1)|     sum(us.d1.s4)|\n" +
                "+----+-------------+------------------+\n" +
                "|   1|         5050|            5060.0|\n" +
                "| 101|        15050|15060.000000000022|\n" +
                "| 201|        25050| 25059.99999999997|\n" +
                "| 301|        35050| 35059.99999999994|\n" +
                "| 401|        45050| 45059.99999999992|\n" +
                "| 501|        55050| 55059.99999999991|\n" +
                "| 601|        65050|  65059.9999999999|\n" +
                "| 701|        75050| 75059.99999999999|\n" +
                "| 801|        85050| 85060.00000000004|\n" +
                "| 901|        94050|  94059.9000000001|\n" +
                "+----+-------------+------------------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+-------------+------------------+\n" +
                "|Time|avg(us.d1.s1)|     avg(us.d1.s4)|\n" +
                "+----+-------------+------------------+\n" +
                "|   1|         50.5|              50.6|\n" +
                "| 101|        150.5|150.60000000000022|\n" +
                "| 201|        250.5| 250.5999999999997|\n" +
                "| 301|        350.5| 350.5999999999994|\n" +
                "| 401|        450.5| 450.5999999999992|\n" +
                "| 501|        550.5| 550.5999999999991|\n" +
                "| 601|        650.5|  650.599999999999|\n" +
                "| 701|        750.5| 750.5999999999999|\n" +
                "| 801|        850.5| 850.6000000000005|\n" +
                "| 901|        950.0| 950.1000000000009|\n" +
                "+----+-------------+------------------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+---------------+---------------+\n" +
                "|Time|count(us.d1.s1)|count(us.d1.s4)|\n" +
                "+----+---------------+---------------+\n" +
                "|   1|            100|            100|\n" +
                "| 101|            100|            100|\n" +
                "| 201|            100|            100|\n" +
                "| 301|            100|            100|\n" +
                "| 401|            100|            100|\n" +
                "| 501|            100|            100|\n" +
                "| 601|            100|            100|\n" +
                "| 701|            100|            100|\n" +
                "| 801|            100|            100|\n" +
                "| 901|             99|             99|\n" +
                "+----+---------------+---------------+\n" +
                "Total line number = 10\n"
        );
        for (int i = 0; i < funcTypeList.size(); i++) {
            String type = funcTypeList.get(i);
            String expected = expectedList.get(i);
            executeAndCompare(String.format(statement, type, type), expected);
        }
    }

    @Test
    public void testRangeDownSampleQuery() {
        String statement = "SELECT %s(s1), %s(s4) FROM us.d1 WHERE time > 600 AND s1 <= 900 GROUP (0, 1000) BY 100ns;";
        List<String> funcTypeList = Arrays.asList(
            "MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT"
        );
        List<String> expectedList = Arrays.asList(
            "ResultSets:\n" +
                "+----+-------------+-------------+\n" +
                "|Time|max(us.d1.s1)|max(us.d1.s4)|\n" +
                "+----+-------------+-------------+\n" +
                "| 601|          700|        700.1|\n" +
                "| 701|          800|        800.1|\n" +
                "| 801|          900|        900.1|\n" +
                "+----+-------------+-------------+\n" +
                "Total line number = 3\n",
            "ResultSets:\n" +
                "+----+-------------+-------------+\n" +
                "|Time|min(us.d1.s1)|min(us.d1.s4)|\n" +
                "+----+-------------+-------------+\n" +
                "| 601|          601|        601.1|\n" +
                "| 701|          701|        701.1|\n" +
                "| 801|          801|        801.1|\n" +
                "+----+-------------+-------------+\n" +
                "Total line number = 3\n",
            "ResultSets:\n" +
                "+----+---------------------+---------------------+\n" +
                "|Time|first_value(us.d1.s1)|first_value(us.d1.s4)|\n" +
                "+----+---------------------+---------------------+\n" +
                "| 601|                  601|                601.1|\n" +
                "| 701|                  701|                701.1|\n" +
                "| 801|                  801|                801.1|\n" +
                "+----+---------------------+---------------------+\n" +
                "Total line number = 3\n",
            "ResultSets:\n" +
                "+----+--------------------+--------------------+\n" +
                "|Time|last_value(us.d1.s1)|last_value(us.d1.s4)|\n" +
                "+----+--------------------+--------------------+\n" +
                "| 601|                 700|               700.1|\n" +
                "| 701|                 800|               800.1|\n" +
                "| 801|                 900|               900.1|\n" +
                "+----+--------------------+--------------------+\n" +
                "Total line number = 3\n",
            "ResultSets:\n" +
                "+----+-------------+-----------------+\n" +
                "|Time|sum(us.d1.s1)|    sum(us.d1.s4)|\n" +
                "+----+-------------+-----------------+\n" +
                "| 601|        65050| 65059.9999999999|\n" +
                "| 701|        75050|75059.99999999999|\n" +
                "| 801|        85050|85060.00000000004|\n" +
                "+----+-------------+-----------------+\n" +
                "Total line number = 3\n",
            "ResultSets:\n" +
                "+----+-------------+-----------------+\n" +
                "|Time|avg(us.d1.s1)|    avg(us.d1.s4)|\n" +
                "+----+-------------+-----------------+\n" +
                "| 601|        650.5| 650.599999999999|\n" +
                "| 701|        750.5|750.5999999999999|\n" +
                "| 801|        850.5|850.6000000000005|\n" +
                "+----+-------------+-----------------+\n" +
                "Total line number = 3\n",
            "ResultSets:\n" +
                "+----+---------------+---------------+\n" +
                "|Time|count(us.d1.s1)|count(us.d1.s4)|\n" +
                "+----+---------------+---------------+\n" +
                "| 601|            100|            100|\n" +
                "| 701|            100|            100|\n" +
                "| 801|            100|            100|\n" +
                "+----+---------------+---------------+\n" +
                "Total line number = 3\n"
        );
        for (int i = 0; i < funcTypeList.size(); i++) {
            String type = funcTypeList.get(i);
            String expected = expectedList.get(i);
            executeAndCompare(String.format(statement, type, type), expected);
        }
    }

    @Test
    public void testDelete() {
        if (!isAbleToDelete) {
            return;
        }
        String delete = "DELETE FROM us.d1.s1 WHERE time > 105 AND time < 115;";
        execute(delete);

        String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE time > 100 AND time < 120;";
        String expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d1.s1|\n" +
            "+----+--------+\n" +
            "| 101|     101|\n" +
            "| 102|     102|\n" +
            "| 103|     103|\n" +
            "| 104|     104|\n" +
            "| 105|     105|\n" +
            "| 115|     115|\n" +
            "| 116|     116|\n" +
            "| 117|     117|\n" +
            "| 118|     118|\n" +
            "| 119|     119|\n" +
            "+----+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(queryOverDeleteRange, expected);

        delete = "DELETE FROM us.d1.s1 WHERE time >= 1126 AND time <= 1155;";
        execute(delete);

        queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE time > 1120 AND time < 1160;";
        expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d1.s1|\n" +
            "+----+--------+\n" +
            "|1121|    1121|\n" +
            "|1122|    1122|\n" +
            "|1123|    1123|\n" +
            "|1124|    1124|\n" +
            "|1125|    1125|\n" +
            "|1156|    1156|\n" +
            "|1157|    1157|\n" +
            "|1158|    1158|\n" +
            "|1159|    1159|\n" +
            "+----+--------+\n" +
            "Total line number = 9\n";
        executeAndCompare(queryOverDeleteRange, expected);

        delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE time > 2236 AND time <= 2265;";
        execute(delete);

        queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE time > 2230 AND time < 2270;";
        expected = "ResultSets:\n" +
            "+----+--------+--------+\n" +
            "|Time|us.d1.s2|us.d1.s4|\n" +
            "+----+--------+--------+\n" +
            "|2231|    2232|  2231.1|\n" +
            "|2232|    2233|  2232.1|\n" +
            "|2233|    2234|  2233.1|\n" +
            "|2234|    2235|  2234.1|\n" +
            "|2235|    2236|  2235.1|\n" +
            "|2236|    2237|  2236.1|\n" +
            "|2266|    2267|  2266.1|\n" +
            "|2267|    2268|  2267.1|\n" +
            "|2268|    2269|  2268.1|\n" +
            "|2269|    2270|  2269.1|\n" +
            "+----+--------+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(queryOverDeleteRange, expected);

        delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE time >= 3346 AND time < 3375;";
        execute(delete);

        queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE time > 3340 AND time < 3380;";
        expected = "ResultSets:\n" +
            "+----+--------+--------+\n" +
            "|Time|us.d1.s2|us.d1.s4|\n" +
            "+----+--------+--------+\n" +
            "|3341|    3342|  3341.1|\n" +
            "|3342|    3343|  3342.1|\n" +
            "|3343|    3344|  3343.1|\n" +
            "|3344|    3345|  3344.1|\n" +
            "|3345|    3346|  3345.1|\n" +
            "|3375|    3376|  3375.1|\n" +
            "|3376|    3377|  3376.1|\n" +
            "|3377|    3378|  3377.1|\n" +
            "|3378|    3379|  3378.1|\n" +
            "|3379|    3380|  3379.1|\n" +
            "+----+--------+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(queryOverDeleteRange, expected);
    }

    @Test
    public void testMultiRangeDelete() {
        if (!isAbleToDelete) {
            return;
        }
        String delete = "DELETE FROM us.d1.s1 WHERE time > 105 AND time < 115 OR time >= 120 AND time <= 230;";
        execute(delete);

        String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE time > 100 AND time < 235;";
        String expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d1.s1|\n" +
            "+----+--------+\n" +
            "| 101|     101|\n" +
            "| 102|     102|\n" +
            "| 103|     103|\n" +
            "| 104|     104|\n" +
            "| 105|     105|\n" +
            "| 115|     115|\n" +
            "| 116|     116|\n" +
            "| 117|     117|\n" +
            "| 118|     118|\n" +
            "| 119|     119|\n" +
            "| 231|     231|\n" +
            "| 232|     232|\n" +
            "| 233|     233|\n" +
            "| 234|     234|\n" +
            "+----+--------+\n" +
            "Total line number = 14\n";
        executeAndCompare(queryOverDeleteRange, expected);

        delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE time > 1115 AND time <= 1125 OR time >= 1130 AND time < 1230;";
        execute(delete);

        queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE time > 1110 AND time < 1235;";
        expected = "ResultSets:\n" +
            "+----+--------+--------+\n" +
            "|Time|us.d1.s2|us.d1.s4|\n" +
            "+----+--------+--------+\n" +
            "|1111|    1112|  1111.1|\n" +
            "|1112|    1113|  1112.1|\n" +
            "|1113|    1114|  1113.1|\n" +
            "|1114|    1115|  1114.1|\n" +
            "|1115|    1116|  1115.1|\n" +
            "|1126|    1127|  1126.1|\n" +
            "|1127|    1128|  1127.1|\n" +
            "|1128|    1129|  1128.1|\n" +
            "|1129|    1130|  1129.1|\n" +
            "|1230|    1231|  1230.1|\n" +
            "|1231|    1232|  1231.1|\n" +
            "|1232|    1233|  1232.1|\n" +
            "|1233|    1234|  1233.1|\n" +
            "|1234|    1235|  1234.1|\n" +
            "+----+--------+--------+\n" +
            "Total line number = 14\n";
        executeAndCompare(queryOverDeleteRange, expected);
    }

    @Test
    public void testCrossRangeDelete() {
        if (!isAbleToDelete) {
            return;
        }
        String delete = "DELETE FROM us.d1.s1 WHERE time > 205 AND time < 215 OR time >= 210 AND time <= 230;";
        execute(delete);

        String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE time > 200 AND time < 235;";
        String expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d1.s1|\n" +
            "+----+--------+\n" +
            "| 201|     201|\n" +
            "| 202|     202|\n" +
            "| 203|     203|\n" +
            "| 204|     204|\n" +
            "| 205|     205|\n" +
            "| 231|     231|\n" +
            "| 232|     232|\n" +
            "| 233|     233|\n" +
            "| 234|     234|\n" +
            "+----+--------+\n" +
            "Total line number = 9\n";
        executeAndCompare(queryOverDeleteRange, expected);

        delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE time > 1115 AND time <= 1125 OR time >= 1120 AND time < 1230;";
        execute(delete);

        queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE time > 1110 AND time < 1235;";
        expected = "ResultSets:\n" +
            "+----+--------+--------+\n" +
            "|Time|us.d1.s2|us.d1.s4|\n" +
            "+----+--------+--------+\n" +
            "|1111|    1112|  1111.1|\n" +
            "|1112|    1113|  1112.1|\n" +
            "|1113|    1114|  1113.1|\n" +
            "|1114|    1115|  1114.1|\n" +
            "|1115|    1116|  1115.1|\n" +
            "|1230|    1231|  1230.1|\n" +
            "|1231|    1232|  1231.1|\n" +
            "|1232|    1233|  1232.1|\n" +
            "|1233|    1234|  1233.1|\n" +
            "|1234|    1235|  1234.1|\n" +
            "+----+--------+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(queryOverDeleteRange, expected);
    }

    @Test
    public void testFromMultiPath() {
        String insert = "INSERT INTO us.d2 (timestamp, s1, s2) values " +
            "(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5), (6, 6, 6);";
        execute(insert);

        insert = "INSERT INTO us.d3 (timestamp, s1, s2) values " +
            "(1, 2, 2), (2, 3, 3), (3, 4, 4), (4, 5, 5), (5, 6, 6), (6, 7, 7);";
        execute(insert);

        String queryFromMultiPath = "SELECT s1, s2 FROM us.d2, us.d3;";
        String expected = "ResultSets:\n" +
            "+----+--------+--------+--------+--------+\n" +
            "|Time|us.d2.s1|us.d2.s2|us.d3.s1|us.d3.s2|\n" +
            "+----+--------+--------+--------+--------+\n" +
            "|   1|       1|       1|       2|       2|\n" +
            "|   2|       2|       2|       3|       3|\n" +
            "|   3|       3|       3|       4|       4|\n" +
            "|   4|       4|       4|       5|       5|\n" +
            "|   5|       5|       5|       6|       6|\n" +
            "|   6|       6|       6|       7|       7|\n" +
            "+----+--------+--------+--------+--------+\n" +
            "Total line number = 6\n";
        executeAndCompare(queryFromMultiPath, expected);

        String queryFromMultiPathWithCondition = "SELECT s1, s2 FROM us.d2, us.d3 WHERE s1 > 2;";
        expected = "ResultSets:\n" +
            "+----+--------+--------+--------+--------+\n" +
            "|Time|us.d2.s1|us.d2.s2|us.d3.s1|us.d3.s2|\n" +
            "+----+--------+--------+--------+--------+\n" +
            "|   3|       3|       3|       4|       4|\n" +
            "|   4|       4|       4|       5|       5|\n" +
            "|   5|       5|       5|       6|       6|\n" +
            "|   6|       6|       6|       7|       7|\n" +
            "+----+--------+--------+--------+--------+\n" +
            "Total line number = 4\n";
        executeAndCompare(queryFromMultiPathWithCondition, expected);

        String queryFromMultiPathWithIntactCondition = "SELECT s1, s2 FROM us.d2, us.d3 WHERE INTACT(us.d2.s1) > 2 AND INTACT(us.d3.s2) < 6;";
        expected = "ResultSets:\n" +
            "+----+--------+--------+--------+--------+\n" +
            "|Time|us.d2.s1|us.d2.s2|us.d3.s1|us.d3.s2|\n" +
            "+----+--------+--------+--------+--------+\n" +
            "|   3|       3|       3|       4|       4|\n" +
            "|   4|       4|       4|       5|       5|\n" +
            "+----+--------+--------+--------+--------+\n" +
            "Total line number = 2\n";
        executeAndCompare(queryFromMultiPathWithIntactCondition, expected);
    }

    @Test
    public void testAlias() {
        // time series alias
        String statement = "SELECT s1 AS rename_series, s2 FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010;";
        String expected = "ResultSets:\n" +
            "+----+-------------+--------+\n" +
            "|Time|rename_series|us.d1.s2|\n" +
            "+----+-------------+--------+\n" +
            "|1000|         1000|    1001|\n" +
            "|1001|         1001|    1002|\n" +
            "|1002|         1002|    1003|\n" +
            "|1003|         1003|    1004|\n" +
            "|1004|         1004|    1005|\n" +
            "|1005|         1005|    1006|\n" +
            "|1006|         1006|    1007|\n" +
            "|1007|         1007|    1008|\n" +
            "|1008|         1008|    1009|\n" +
            "|1009|         1009|    1010|\n" +
            "+----+-------------+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);

        // result set alias
        statement = "SELECT s1, s2 FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010 AS rename_result_set;";
        expected = "ResultSets:\n" +
            "+----+--------------------------+--------------------------+\n" +
            "|Time|rename_result_set.us.d1.s1|rename_result_set.us.d1.s2|\n" +
            "+----+--------------------------+--------------------------+\n" +
            "|1000|                      1000|                      1001|\n" +
            "|1001|                      1001|                      1002|\n" +
            "|1002|                      1002|                      1003|\n" +
            "|1003|                      1003|                      1004|\n" +
            "|1004|                      1004|                      1005|\n" +
            "|1005|                      1005|                      1006|\n" +
            "|1006|                      1006|                      1007|\n" +
            "|1007|                      1007|                      1008|\n" +
            "|1008|                      1008|                      1009|\n" +
            "|1009|                      1009|                      1010|\n" +
            "+----+--------------------------+--------------------------+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);

        // time series and result set alias
        statement = "SELECT s1 AS rename_series, s2 FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010 AS rename_result_set;";
        expected = "ResultSets:\n" +
            "+----+-------------------------------+--------------------------+\n" +
            "|Time|rename_result_set.rename_series|rename_result_set.us.d1.s2|\n" +
            "+----+-------------------------------+--------------------------+\n" +
            "|1000|                           1000|                      1001|\n" +
            "|1001|                           1001|                      1002|\n" +
            "|1002|                           1002|                      1003|\n" +
            "|1003|                           1003|                      1004|\n" +
            "|1004|                           1004|                      1005|\n" +
            "|1005|                           1005|                      1006|\n" +
            "|1006|                           1006|                      1007|\n" +
            "|1007|                           1007|                      1008|\n" +
            "|1008|                           1008|                      1009|\n" +
            "|1009|                           1009|                      1010|\n" +
            "+----+-------------------------------+--------------------------+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testAggregateSubQuery() {
        String statement = "SELECT %s_s1 FROM (SELECT %s(s1) AS %s_s1 FROM us.d1 GROUP [1000, 1600) BY 60ns);";
        List<String> funcTypeList = Arrays.asList(
            "max", "min", "sum", "avg", "count", "first_value", "last_value"
        );

        List<String> expectedList = Arrays.asList(
            "ResultSets:\n" +
                "+----+------+\n" +
                "|Time|max_s1|\n" +
                "+----+------+\n" +
                "|1000|  1059|\n" +
                "|1060|  1119|\n" +
                "|1120|  1179|\n" +
                "|1180|  1239|\n" +
                "|1240|  1299|\n" +
                "|1300|  1359|\n" +
                "|1360|  1419|\n" +
                "|1420|  1479|\n" +
                "|1480|  1539|\n" +
                "|1540|  1599|\n" +
                "+----+------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+------+\n" +
                "|Time|min_s1|\n" +
                "+----+------+\n" +
                "|1000|  1000|\n" +
                "|1060|  1060|\n" +
                "|1120|  1120|\n" +
                "|1180|  1180|\n" +
                "|1240|  1240|\n" +
                "|1300|  1300|\n" +
                "|1360|  1360|\n" +
                "|1420|  1420|\n" +
                "|1480|  1480|\n" +
                "|1540|  1540|\n" +
                "+----+------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+------+\n" +
                "|Time|sum_s1|\n" +
                "+----+------+\n" +
                "|1000| 61770|\n" +
                "|1060| 65370|\n" +
                "|1120| 68970|\n" +
                "|1180| 72570|\n" +
                "|1240| 76170|\n" +
                "|1300| 79770|\n" +
                "|1360| 83370|\n" +
                "|1420| 86970|\n" +
                "|1480| 90570|\n" +
                "|1540| 94170|\n" +
                "+----+------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+------+\n" +
                "|Time|avg_s1|\n" +
                "+----+------+\n" +
                "|1000|1029.5|\n" +
                "|1060|1089.5|\n" +
                "|1120|1149.5|\n" +
                "|1180|1209.5|\n" +
                "|1240|1269.5|\n" +
                "|1300|1329.5|\n" +
                "|1360|1389.5|\n" +
                "|1420|1449.5|\n" +
                "|1480|1509.5|\n" +
                "|1540|1569.5|\n" +
                "+----+------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+--------+\n" +
                "|Time|count_s1|\n" +
                "+----+--------+\n" +
                "|1000|      60|\n" +
                "|1060|      60|\n" +
                "|1120|      60|\n" +
                "|1180|      60|\n" +
                "|1240|      60|\n" +
                "|1300|      60|\n" +
                "|1360|      60|\n" +
                "|1420|      60|\n" +
                "|1480|      60|\n" +
                "|1540|      60|\n" +
                "+----+--------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+--------------+\n" +
                "|Time|first_value_s1|\n" +
                "+----+--------------+\n" +
                "|1000|          1000|\n" +
                "|1060|          1060|\n" +
                "|1120|          1120|\n" +
                "|1180|          1180|\n" +
                "|1240|          1240|\n" +
                "|1300|          1300|\n" +
                "|1360|          1360|\n" +
                "|1420|          1420|\n" +
                "|1480|          1480|\n" +
                "|1540|          1540|\n" +
                "+----+--------------+\n" +
                "Total line number = 10\n",
            "ResultSets:\n" +
                "+----+-------------+\n" +
                "|Time|last_value_s1|\n" +
                "+----+-------------+\n" +
                "|1000|         1059|\n" +
                "|1060|         1119|\n" +
                "|1120|         1179|\n" +
                "|1180|         1239|\n" +
                "|1240|         1299|\n" +
                "|1300|         1359|\n" +
                "|1360|         1419|\n" +
                "|1420|         1479|\n" +
                "|1480|         1539|\n" +
                "|1540|         1599|\n" +
                "+----+-------------+\n" +
                "Total line number = 10\n"
        );
        for (int i = 0; i < funcTypeList.size(); i++) {
            String type = funcTypeList.get(i);
            String expected = expectedList.get(i);
            executeAndCompare(String.format(statement, type, type, type), expected);
        }
    }

    @Test
    public void testValueFilterSubQuery() {
        String statement = "SELECT ts FROM (SELECT s1 AS ts FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010);";
        String expected = "ResultSets:\n" +
            "+----+----+\n" +
            "|Time|  ts|\n" +
            "+----+----+\n" +
            "|1000|1000|\n" +
            "|1001|1001|\n" +
            "|1002|1002|\n" +
            "|1003|1003|\n" +
            "|1004|1004|\n" +
            "|1005|1005|\n" +
            "|1006|1006|\n" +
            "|1007|1007|\n" +
            "|1008|1008|\n" +
            "|1009|1009|\n" +
            "+----+----+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);

        statement = "SELECT avg_s1 FROM (SELECT AVG(s1) AS avg_s1 FROM us.d1 GROUP [1000, 1600) BY 100ns) WHERE avg_s1 > 1200;";
        expected = "ResultSets:\n" +
            "+----+------+\n" +
            "|Time|avg_s1|\n" +
            "+----+------+\n" +
            "|1200|1249.5|\n" +
            "|1300|1349.5|\n" +
            "|1400|1449.5|\n" +
            "|1500|1549.5|\n" +
            "+----+------+\n" +
            "Total line number = 4\n";
        executeAndCompare(statement, expected);

        statement = "SELECT avg_s1 FROM (SELECT AVG(s1) AS avg_s1 FROM us.d1 WHERE s1 < 1500 GROUP [1000, 1600) BY 100ns) WHERE avg_s1 > 1200;";
        expected = "ResultSets:\n" +
            "+----+------+\n" +
            "|Time|avg_s1|\n" +
            "+----+------+\n" +
            "|1200|1249.5|\n" +
            "|1300|1349.5|\n" +
            "|1400|1449.5|\n" +
            "+----+------+\n" +
            "Total line number = 3\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testMultiSubQuery() {
        String statement = "SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 GROUP [1000, 1100) BY 10ns;";
        String expected = "ResultSets:\n" +
            "+----+------+------+\n" +
            "|Time|avg_s1|sum_s2|\n" +
            "+----+------+------+\n" +
            "|1000|1004.5| 10055|\n" +
            "|1010|1014.5| 10155|\n" +
            "|1020|1024.5| 10255|\n" +
            "|1030|1034.5| 10355|\n" +
            "|1040|1044.5| 10455|\n" +
            "|1050|1054.5| 10555|\n" +
            "|1060|1064.5| 10655|\n" +
            "|1070|1074.5| 10755|\n" +
            "|1080|1084.5| 10855|\n" +
            "|1090|1094.5| 10955|\n" +
            "+----+------+------+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);

        statement = "SELECT avg_s1, sum_s2 FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 GROUP [1000, 1100) BY 10ns) WHERE avg_s1 > 1020 AND sum_s2 < 10800;";
        expected = "ResultSets:\n" +
            "+----+------+------+\n" +
            "|Time|avg_s1|sum_s2|\n" +
            "+----+------+------+\n" +
            "|1020|1024.5| 10255|\n" +
            "|1030|1034.5| 10355|\n" +
            "|1040|1044.5| 10455|\n" +
            "|1050|1054.5| 10555|\n" +
            "|1060|1064.5| 10655|\n" +
            "|1070|1074.5| 10755|\n" +
            "+----+------+------+\n" +
            "Total line number = 6\n";
        executeAndCompare(statement, expected);

        statement = "SELECT MAX(avg_s1), MIN(sum_s2) FROM (SELECT avg_s1, sum_s2 FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 GROUP [1000, 1100) BY 10ns) WHERE avg_s1 > 1020 AND sum_s2 < 10800);";
        expected = "ResultSets:\n" +
            "+-----------+-----------+\n" +
            "|max(avg_s1)|min(sum_s2)|\n" +
            "+-----------+-----------+\n" +
            "|     1074.5|      10255|\n" +
            "+-----------+-----------+\n" +
            "Total line number = 1\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testDateFormat() {
        if (!isAbleToDelete) {
            return;
        }
        String insert = "INSERT INTO us.d2(TIME, date) VALUES (%s, %s);";
        List<String> dateFormats = Arrays.asList(
            "2021-08-26 16:15:27",
            "2021/08/26 16:15:28",
            "2021.08.26 16:15:29",
            "2021-08-26T16:15:30",
            "2021/08/26T16:15:31",
            "2021.08.26T16:15:32",

            "2021-08-26 16:15:27.001",
            "2021/08/26 16:15:28.001",
            "2021.08.26 16:15:29.001",
            "2021-08-26T16:15:30.001",
            "2021/08/26T16:15:31.001",
            "2021.08.26T16:15:32.001"
        );

        for (int i = 0; i < dateFormats.size(); i++) {
            execute(String.format(insert, dateFormats.get(i), i));
        }

        String query = "SELECT date FROM us.d2;";
        String expected =
            "ResultSets:\n"
                + "+-------------------+----------+\n"
                + "|               Time|us.d2.date|\n"
                + "+-------------------+----------+\n"
                + "|1629965727000000000|         0|\n"
                + "|1629965727001000000|         6|\n"
                + "|1629965728000000000|         1|\n"
                + "|1629965728001000000|         7|\n"
                + "|1629965729000000000|         2|\n"
                + "|1629965729001000000|         8|\n"
                + "|1629965730000000000|         3|\n"
                + "|1629965730001000000|         9|\n"
                + "|1629965731000000000|         4|\n"
                + "|1629965731001000000|        10|\n"
                + "|1629965732000000000|         5|\n"
                + "|1629965732001000000|        11|\n"
                + "+-------------------+----------+\n"
                + "Total line number = 12\n";
        executeAndCompare(query, expected);

        query = "SELECT date FROM us.d2 WHERE time >= 2021-08-26 16:15:27 AND time <= 2021.08.26T16:15:32.001;";
        expected =
            "ResultSets:\n"
                + "+-------------------+----------+\n"
                + "|               Time|us.d2.date|\n"
                + "+-------------------+----------+\n"
                + "|1629965727000000000|         0|\n"
                + "|1629965727001000000|         6|\n"
                + "|1629965728000000000|         1|\n"
                + "|1629965728001000000|         7|\n"
                + "|1629965729000000000|         2|\n"
                + "|1629965729001000000|         8|\n"
                + "|1629965730000000000|         3|\n"
                + "|1629965730001000000|         9|\n"
                + "|1629965731000000000|         4|\n"
                + "|1629965731001000000|        10|\n"
                + "|1629965732000000000|         5|\n"
                + "|1629965732001000000|        11|\n"
                + "+-------------------+----------+\n"
                + "Total line number = 12\n";
        executeAndCompare(query, expected);

        query = "SELECT date FROM us.d2 WHERE time >= 2021.08.26 16:15:29 AND time <= 2021-08-26T16:15:30.001;";
        expected =
            "ResultSets:\n"
                + "+-------------------+----------+\n"
                + "|               Time|us.d2.date|\n"
                + "+-------------------+----------+\n"
                + "|1629965729000000000|         2|\n"
                + "|1629965729001000000|         8|\n"
                + "|1629965730000000000|         3|\n"
                + "|1629965730001000000|         9|\n"
                + "+-------------------+----------+\n"
                + "Total line number = 4\n";
        executeAndCompare(query, expected);

        query = "SELECT date FROM us.d2 WHERE time >= 2021/08/26 16:15:28 AND time <= 2021/08/26T16:15:31.001;";
        expected =
            "ResultSets:\n"
                + "+-------------------+----------+\n"
                + "|               Time|us.d2.date|\n"
                + "+-------------------+----------+\n"
                + "|1629965728000000000|         1|\n"
                + "|1629965728001000000|         7|\n"
                + "|1629965729000000000|         2|\n"
                + "|1629965729001000000|         8|\n"
                + "|1629965730000000000|         3|\n"
                + "|1629965730001000000|         9|\n"
                + "|1629965731000000000|         4|\n"
                + "|1629965731001000000|        10|\n"
                + "+-------------------+----------+\n"
                + "Total line number = 8\n";
        executeAndCompare(query, expected);
    }

    @Test
    public void testInsertWithSubQuery() {
        String insert = "INSERT INTO us.d2(TIME, s1) VALUES (SELECT s1 FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010);";
        execute(insert);

        String query = "SELECT s1 FROM us.d2;";
        String expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d2.s1|\n" +
            "+----+--------+\n" +
            "|1000|    1000|\n" +
            "|1001|    1001|\n" +
            "|1002|    1002|\n" +
            "|1003|    1003|\n" +
            "|1004|    1004|\n" +
            "|1005|    1005|\n" +
            "|1006|    1006|\n" +
            "|1007|    1007|\n" +
            "|1008|    1008|\n" +
            "|1009|    1009|\n" +
            "+----+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(query, expected);

        insert = "INSERT INTO us.d3(TIME, s1) VALUES (SELECT s1 FROM us.d1 WHERE s1 >= 1000 AND s1 < 1010) TIME_OFFSET = 100;";
        execute(insert);

        query = "SELECT s1 FROM us.d3;";
        expected = "ResultSets:\n" +
            "+----+--------+\n" +
            "|Time|us.d3.s1|\n" +
            "+----+--------+\n" +
            "|1100|    1000|\n" +
            "|1101|    1001|\n" +
            "|1102|    1002|\n" +
            "|1103|    1003|\n" +
            "|1104|    1004|\n" +
            "|1105|    1005|\n" +
            "|1106|    1006|\n" +
            "|1107|    1007|\n" +
            "|1108|    1008|\n" +
            "|1109|    1009|\n" +
            "+----+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(query, expected);

        insert = "INSERT INTO us.d4(TIME, s1, s2) VALUES (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 GROUP [1000, 1100) BY 10ns);";
        execute(insert);

        query = "SELECT s1, s2 FROM us.d4";
        expected = "ResultSets:\n" +
            "+----+--------+--------+\n" +
            "|Time|us.d4.s1|us.d4.s2|\n" +
            "+----+--------+--------+\n" +
            "|1000|  1004.5|   10055|\n" +
            "|1010|  1014.5|   10155|\n" +
            "|1020|  1024.5|   10255|\n" +
            "|1030|  1034.5|   10355|\n" +
            "|1040|  1044.5|   10455|\n" +
            "|1050|  1054.5|   10555|\n" +
            "|1060|  1064.5|   10655|\n" +
            "|1070|  1074.5|   10755|\n" +
            "|1080|  1084.5|   10855|\n" +
            "|1090|  1094.5|   10955|\n" +
            "+----+--------+--------+\n" +
            "Total line number = 10\n";
        executeAndCompare(query, expected);

        insert = "INSERT INTO us.d5(TIME, s1, s2) VALUES (SELECT avg_s1, sum_s2 FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 GROUP [1000, 1100) BY 10ns) WHERE avg_s1 > 1020 AND sum_s2 < 10800);";
        execute(insert);

        query = "SELECT s1, s2 FROM us.d5";
        expected = "ResultSets:\n" +
            "+----+--------+--------+\n" +
            "|Time|us.d5.s1|us.d5.s2|\n" +
            "+----+--------+--------+\n" +
            "|1020|  1024.5|   10255|\n" +
            "|1030|  1034.5|   10355|\n" +
            "|1040|  1044.5|   10455|\n" +
            "|1050|  1054.5|   10555|\n" +
            "|1060|  1064.5|   10655|\n" +
            "|1070|  1074.5|   10755|\n" +
            "+----+--------+--------+\n" +
            "Total line number = 6\n";
        executeAndCompare(query, expected);

        insert = "INSERT INTO us.d6(TIME, s1, s2) VALUES (SELECT MAX(avg_s1), MIN(sum_s2) FROM (SELECT avg_s1, sum_s2 FROM (SELECT AVG(s1) AS avg_s1, SUM(s2) AS sum_s2 FROM us.d1 GROUP [1000, 1100) BY 10ns) WHERE avg_s1 > 1020 AND sum_s2 < 10800));";
        execute(insert);

        query = "SELECT s1, s2 FROM us.d6";
        expected =
            "ResultSets:\n"
                + "+----+--------+--------+\n"
                + "|Time|us.d6.s1|us.d6.s2|\n"
                + "+----+--------+--------+\n"
                + "|   0|  1074.5|   10255|\n"
                + "+----+--------+--------+\n"
                + "Total line number = 1\n";
        executeAndCompare(query, expected);
    }

    @Test
    public void testSpecialPath() {
        if (!isSupportSpecialPath) {
            return;
        }
        // Chinese path
        String insert = "INSERT INTO 测试.前缀(TIME, 后缀) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
        execute(insert);

        String query = "SELECT 后缀 FROM 测试.前缀;";
        String expected =
            "ResultSets:\n"
                + "+----+--------+\n"
                + "|Time|测试.前缀.后缀|\n"
                + "+----+--------+\n"
                + "|   1|       1|\n"
                + "|   2|       2|\n"
                + "|   3|       3|\n"
                + "|   4|       4|\n"
                + "|   5|       5|\n"
                + "+----+--------+\n"
                + "Total line number = 5\n";
        executeAndCompare(query, expected);

        // number path
        insert = "INSERT INTO 114514(TIME, 1919810) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
        execute(insert);

        query = "SELECT 1919810 FROM 114514;";
        expected =
            "ResultSets:\n"
                + "+----+--------------+\n"
                + "|Time|114514.1919810|\n"
                + "+----+--------------+\n"
                + "|   1|             1|\n"
                + "|   2|             2|\n"
                + "|   3|             3|\n"
                + "|   4|             4|\n"
                + "|   5|             5|\n"
                + "+----+--------------+\n"
                + "Total line number = 5\n";
        executeAndCompare(query, expected);

        // special symbol path
        insert = "INSERT INTO _:/@#$%&+(TIME, _:/@#$%&+) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
        execute(insert);

        query = "SELECT _:/@#$%&+ FROM _:/@#$%&+;";
        expected =
            "ResultSets:\n"
                + "+----+-------------------+\n"
                + "|Time|_:/@#$%&+._:/@#$%&+|\n"
                + "+----+-------------------+\n"
                + "|   1|                  1|\n"
                + "|   2|                  2|\n"
                + "|   3|                  3|\n"
                + "|   4|                  4|\n"
                + "|   5|                  5|\n"
                + "+----+-------------------+\n"
                + "Total line number = 5\n";
        executeAndCompare(query, expected);

        // mix path
        insert = "INSERT INTO 测试.前缀.114514(TIME, 1919810._:/@#$%&+.后缀) VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);";
        execute(insert);

        query = "SELECT 1919810._:/@#$%&+.后缀 FROM 测试.前缀.114514;";
        expected =
            "ResultSets:\n"
                + "+----+---------------------------------+\n"
                + "|Time|测试.前缀.114514.1919810._:/@#$%&+.后缀|\n"
                + "+----+---------------------------------+\n"
                + "|   1|                                1|\n"
                + "|   2|                                2|\n"
                + "|   3|                                3|\n"
                + "|   4|                                4|\n"
                + "|   5|                                5|\n"
                + "+----+---------------------------------+\n"
                + "Total line number = 5\n";
        executeAndCompare(query, expected);
    }

    @Test
    public void testErrorClause() {
        String errClause = "DELETE FROM us.d1.s1 WHERE time > 105 AND time < 115 AND time >= 120 AND time <= 230;";
        executeAndCompareErrMsg(errClause, "This clause delete nothing, check your filter again.");

        errClause = "DELETE FROM us.d1.s1 WHERE time > 105 AND time < 115 AND s1 < 10;";
        executeAndCompareErrMsg(errClause, "delete clause can not use value or path filter.");

        errClause = "DELETE FROM us.d1.s1 WHERE time != 105;";
        executeAndCompareErrMsg(errClause, "Not support [!=] in delete clause.");

        errClause = "SELECT s1 FROM us.d1 GROUP (0, 1000) BY 100ms;";
        executeAndCompareErrMsg(errClause, "Group by clause cannot be used without aggregate function.");

        errClause = "SELECT last(s1), max(s2) FROM us.d1;";
        executeAndCompareErrMsg(errClause, "SetToSet/SetToRow/RowToRow functions can not be mixed in aggregate query.");

        errClause = "SELECT s1 FROM us.d1 GROUP (100, 10) BY 100ms;";
        executeAndCompareErrMsg(errClause, "Start time should be smaller than endTime in time interval.");

        errClause = "SELECT min(s1), max(s2) FROM us.d1 ORDER BY TIME;";
        executeAndCompareErrMsg(errClause, "Not support ORDER BY clause in aggregate query.");
    }

    @Test
    public void testDeleteTimeSeries() {
        if (!isAbleToDelete) {
            return;
        }
        String showTimeSeries = "SHOW TIME SERIES;";
        String expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s1|    LONG|\n"
                + "|us.d1.s2|    LONG|\n"
                + "|us.d1.s3|  BINARY|\n"
                + "|us.d1.s4|  DOUBLE|\n"
                + "+--------+--------+\n"
                + "Total line number = 4\n";
        executeAndCompare(showTimeSeries, expected);

        String deleteTimeSeries = "DELETE TIME SERIES us.d1.s4";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES;";
        expected =
            "Time series:\n"
                + "+--------+--------+\n"
                + "|    Path|DataType|\n"
                + "+--------+--------+\n"
                + "|us.d1.s1|    LONG|\n"
                + "|us.d1.s2|    LONG|\n"
                + "|us.d1.s3|  BINARY|\n"
                + "+--------+--------+\n"
                + "Total line number = 3\n";
        executeAndCompare(showTimeSeries, expected);

        String showTimeSeriesData = "SELECT s4 FROM us.d1;";
        expected = "ResultSets:\n" +
            "+----+\n" +
            "|Time|\n" +
            "+----+\n" +
            "+----+\n" +
            "Empty set.\n";
        executeAndCompare(showTimeSeriesData, expected);

        deleteTimeSeries = "DELETE TIME SERIES us.*";
        execute(deleteTimeSeries);

        showTimeSeries = "SHOW TIME SERIES;";
        expected =
            "Time series:\n"
                + "+----+--------+\n"
                + "|Path|DataType|\n"
                + "+----+--------+\n"
                + "+----+--------+\n"
                + "Empty set.\n";
        executeAndCompare(showTimeSeries, expected);

        showTimeSeriesData = "SELECT * FROM *;";
        expected = "ResultSets:\n" +
            "+----+\n" +
            "|Time|\n" +
            "+----+\n" +
            "+----+\n" +
            "Empty set.\n";
        executeAndCompare(showTimeSeriesData, expected);

        String countPoints = "COUNT POINTS";
        expected = "Points num: 0\n";
        executeAndCompare(countPoints, expected);
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
