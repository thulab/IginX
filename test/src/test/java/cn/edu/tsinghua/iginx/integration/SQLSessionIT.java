package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class SQLSessionIT {

    private static final Logger logger = LoggerFactory.getLogger(SQLSessionIT.class);

    private static Session session;

    protected boolean isAbleToDelete;

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
        String insertStrPrefix = "INSERT INTO us.d1 (timestamp, s1, s2, s3, s4) values ";

        long startTimestamp = 0L;
        long endTimestamp = 15000L;

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

    private void executeAndCompareErrMsg(String statement, String exceptErrMsg) {
        logger.info("Execute Statement: \"{}\"", statement);

        try {
            session.executeSql(statement);
        } catch (SessionException | ExecutionException e) {
            logger.info("Statement: \"{}\" execute fail. Because: {}", statement, e.getMessage());
            assertEquals(exceptErrMsg, e.getMessage());
        }
    }

    @Test
    public void testCountPath() {
        String statement = "SELECT COUNT(*) FROM us.d1;";
        String excepted = "ResultSets:\n" +
                "+---------------+---------------+---------------+---------------+\n" +
                "|count(us.d1.s1)|count(us.d1.s2)|count(us.d1.s3)|count(us.d1.s4)|\n" +
                "+---------------+---------------+---------------+---------------+\n" +
                "|          15000|          15000|          15000|          15000|\n" +
                "+---------------+---------------+---------------+---------------+\n" +
                "Total line number = 1\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testCountPoints() {
        String statement = "COUNT POINTS;";
        String excepted = "Points num: 60000\n";
        executeAndCompare(statement, excepted);
    }

//    @Test
//    public void testShowTimeSeries() {
//        String statement = "SHOW TIME SERIES;";
//        String excepted = "Time series:\n" +
//                "+--------+--------+\n" +
//                "|    Path|DataType|\n" +
//                "+--------+--------+\n" +
//                "|us.d1.s1|    LONG|\n" +
//                "|us.d1.s3|  BINARY|\n" +
//                "|us.d1.s2|    LONG|\n" +
//                "|us.d1.s4|  DOUBLE|\n" +
//                "+--------+--------+\n" +
//                "Total line number = 4\n";
//        executeAndCompare(statement, excepted);
//    }

    @Test
    public void testShowReplicaNum() {
        String statement = "SHOW REPLICA NUMBER;";
        String excepted = "Replica num: 2\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testTimeRangeQuery() {
        String statement = "SELECT s1 FROM us.d1 WHERE time > 100 AND time < 120;";
        String excepted = "ResultSets:\n" +
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
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testValueFilter() {
        String statement = "SELECT s1 FROM us.d1 WHERE time > 0 AND time < 10000 and s1 > 200 and s1 < 210;";
        String excepted = "ResultSets:\n" +
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
        executeAndCompare(statement, excepted);
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
        String statement = "SELECT LAST(s2), LAST(s4) FROM us.d1 WHERE time > 0;";
        String expected = "ResultSets:\n" +
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

        statement = "SELECT FIRST(s2), FIRST(s4) FROM us.d1 WHERE time > 1000;";
        expected = "ResultSets:\n" +
                "+----+--------+------+\n" +
                "|Time|    path| value|\n" +
                "+----+--------+------+\n" +
                "|1001|us.d1.s2|  1002|\n" +
                "|1001|us.d1.s4|1001.1|\n" +
                "+----+--------+------+\n" +
                "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testAggregateQuery() {
        String statement = "SELECT %s(s1), %s(s2) FROM us.d1 WHERE time > 0 AND time < 1000;";
        List<String> funcTypeList = Arrays.asList(
                "MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT"
        );
        List<String> exceptedList = Arrays.asList(
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
            String excepted = exceptedList.get(i);
            executeAndCompare(String.format(statement, type, type), excepted);
        }
    }

    @Test
    public void testDownSampleQuery() {
        String statement = "SELECT %s(s1), %s(s4) FROM us.d1 GROUP (0, 1000) BY 100ms;";
        List<String> funcTypeList = Arrays.asList(
                "MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT"
        );
        List<String> exceptedList = Arrays.asList(
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
            String excepted = exceptedList.get(i);
            executeAndCompare(String.format(statement, type, type), excepted);
        }
    }

    @Test
    public void testRangeDownSampleQuery() {
        String statement = "SELECT %s(s1), %s(s4) FROM us.d1 WHERE time > 600 AND s1 <= 900 GROUP (0, 1000) BY 100ms;";
        List<String> funcTypeList = Arrays.asList(
                "MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT"
        );
        List<String> exceptedList = Arrays.asList(
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
            String excepted = exceptedList.get(i);
            executeAndCompare(String.format(statement, type, type), excepted);
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
        String excepted = "ResultSets:\n" +
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
        executeAndCompare(queryOverDeleteRange, excepted);

        delete = "DELETE FROM us.d1.s1 WHERE time >= 1126 AND time <= 1155;";
        execute(delete);

        queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE time > 1120 AND time < 1160;";
        excepted = "ResultSets:\n" +
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
        executeAndCompare(queryOverDeleteRange, excepted);

        delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE time > 2236 AND time <= 2265;";
        execute(delete);

        queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE time > 2230 AND time < 2270;";
        excepted = "ResultSets:\n" +
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
        executeAndCompare(queryOverDeleteRange, excepted);

        delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE time >= 3346 AND time < 3375;";
        execute(delete);

        queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE time > 3340 AND time < 3380;";
        excepted = "ResultSets:\n" +
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
        executeAndCompare(queryOverDeleteRange, excepted);
    }

    @Test
    public void testMultiRangeDelete() {
        if (!isAbleToDelete) {
            return;
        }
        String delete = "DELETE FROM us.d1.s1 WHERE time > 105 AND time < 115 OR time >= 120 AND time <= 230;";
        execute(delete);

        String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE time > 100 AND time < 235;";
        String excepted = "ResultSets:\n" +
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
        executeAndCompare(queryOverDeleteRange, excepted);

        delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE time > 1115 AND time <= 1125 OR time >= 1130 AND time < 1230;";
        execute(delete);

        queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE time > 1110 AND time < 1235;";
        excepted = "ResultSets:\n" +
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
        executeAndCompare(queryOverDeleteRange, excepted);
    }

    @Test
    public void testCrossRangeDelete() {
        if (!isAbleToDelete) {
            return;
        }
        String delete = "DELETE FROM us.d1.s1 WHERE time > 205 AND time < 215 OR time >= 210 AND time <= 230;";
        execute(delete);

        String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE time > 200 AND time < 235;";
        String excepted = "ResultSets:\n" +
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
        executeAndCompare(queryOverDeleteRange, excepted);

        delete = "DELETE FROM us.d1.s2, us.d1.s4 WHERE time > 1115 AND time <= 1125 OR time >= 1120 AND time < 1230;";
        execute(delete);

        queryOverDeleteRange = "SELECT s2, s4 FROM us.d1 WHERE time > 1110 AND time < 1235;";
        excepted = "ResultSets:\n" +
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
        executeAndCompare(queryOverDeleteRange, excepted);
    }

    @Test
    public void testErrorClause() {
        String errClause = "DELETE FROM us.d1.s1 WHERE time > 105 AND time < 115 AND time >= 120 AND time <= 230;";
        executeAndCompareErrMsg(errClause, "This clause delete nothing, check your filter again.");

        errClause = "DELETE FROM us.d1.s1 WHERE time > 105 AND time < 115 AND s1 < 10;";
        executeAndCompareErrMsg(errClause, "delete clause can not use value filter.");

        errClause = "DELETE FROM us.d1.s1 WHERE time != 105;";
        executeAndCompareErrMsg(errClause, "Not support [!=] in delete clause.");

        errClause = "SELECT s1 FROM us.d1 GROUP (0, 1000) BY 100ms;";
        executeAndCompareErrMsg(errClause, "Group by clause cannot be used without aggregate function.");

        errClause = "SELECT last(s1), max(s2) FROM us.d1;";
        executeAndCompareErrMsg(errClause, "First/Last query and other aggregate queries can not be mixed.");

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
        String deleteTimeSeries = "DELETE TIME SERIES us.*";
        execute(deleteTimeSeries);

        String showTimeSeries = "SELECT * FROM *;";
        String excepted = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";
        executeAndCompare(showTimeSeries, excepted);

        String countPoints = "COUNT POINTS";
        excepted = "Points num: 0\n";
        executeAndCompare(countPoints, excepted);
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
