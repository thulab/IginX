package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SQLSessionIT {

    private static final Logger logger = LoggerFactory.getLogger(SQLSessionIT.class);

    private static Session session;

    @Before
    public void setUp() {
        session = new Session("127.0.0.1", 6888, "root", "root");
        try {
            session.openSession();
            insertData();
        } catch (SessionException | ExecutionException e) {
            logger.error(e.getMessage());
        }
    }

    @After
    public void tearDown() {
        try {
            clearData();
            session.closeSession();
        } catch (SessionException | ExecutionException e) {
            logger.error(e.getMessage());
        }
    }

    private void insertData() throws ExecutionException, SessionException {
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

    private void clearData() throws ExecutionException, SessionException {
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
    public void testCountPath() {
        String statement = "SELECT COUNT(*) FROM us.d1;";
        String excepted = "AggregateQuery ResultSets:\n" +
                "+---------------+---------------+---------------+---------------+\n" +
                "|COUNT(us.d1.s3)|COUNT(us.d1.s1)|COUNT(us.d1.s2)|COUNT(us.d1.s4)|\n" +
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

    @Test
    public void testShowTimeSeries() {
        String statement = "SHOW TIME SERIES;";
        String excepted = "Time series:\n" +
                "+--------+--------+\n" +
                "|    Path|DataType|\n" +
                "+--------+--------+\n" +
                "|us.d1.s1|    LONG|\n" +
                "|us.d1.s3|  BINARY|\n" +
                "|us.d1.s2|    LONG|\n" +
                "|us.d1.s4|  DOUBLE|\n" +
                "+--------+--------+\n" +
                "Total line number = 4\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testShowReplicaNum() {
        String statement = "SHOW REPLICA NUMBER;";
        String excepted = "Replica num: 2\n";
        executeAndCompare(statement, excepted);
    }

    @Test
    public void testTimeRangeQuery() {
        String statement = "SELECT s1 FROM us.d1 WHERE time in (100, 120);";
        String excepted = "SimpleQuery ResultSets:\n" +
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
        String statement = "SELECT s1 FROM us.d1 WHERE time in (0, 10000) and s1 > 200 and s1 < 210;";
        String excepted = "ValueFilterQuery ResultSets:\n" +
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
    public void testLastQuery() {
        String statement = "SELECT LAST(s2), LAST(s4) FROM us.d1 WHERE time in (0, INF);";
        String expected = "LastQuery ResultSets:\n" +
                "+-----+--------+-------+\n" +
                "| Time|    Path|  value|\n" +
                "+-----+--------+-------+\n" +
                "|14999|us.d1.s2|  15000|\n" +
                "|14999|us.d1.s4|14999.1|\n" +
                "+-----+--------+-------+\n" +
                "Total line number = 2\n";
        executeAndCompare(statement, expected);
    }

    @Test
    public void testAggregateQuery() {
        String statement = "SELECT %s(s1), %s(s2) FROM us.d1 WHERE time in (0, 1000);";
        List<String> funcTypeList = Arrays.asList(
                "MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT"
        );
        List<String> exceptedList = Arrays.asList(
                "AggregateQuery ResultSets:\n" +
                        "+-------------+-------------+\n" +
                        "|MAX(us.d1.s1)|MAX(us.d1.s2)|\n" +
                        "+-------------+-------------+\n" +
                        "|          999|         1000|\n" +
                        "+-------------+-------------+\n" +
                        "Total line number = 1\n",
                "AggregateQuery ResultSets:\n" +
                        "+-------------+-------------+\n" +
                        "|MIN(us.d1.s1)|MIN(us.d1.s2)|\n" +
                        "+-------------+-------------+\n" +
                        "|            1|            2|\n" +
                        "+-------------+-------------+\n" +
                        "Total line number = 1\n",
                "AggregateQuery ResultSets:\n" +
                        "+---------------------+---------------------+\n" +
                        "|FIRST_VALUE(us.d1.s1)|FIRST_VALUE(us.d1.s2)|\n" +
                        "+---------------------+---------------------+\n" +
                        "|                    1|                    2|\n" +
                        "+---------------------+---------------------+\n" +
                        "Total line number = 1\n",
                "AggregateQuery ResultSets:\n" +
                        "+--------------------+--------------------+\n" +
                        "|LAST_VALUE(us.d1.s1)|LAST_VALUE(us.d1.s2)|\n" +
                        "+--------------------+--------------------+\n" +
                        "|                 999|                1000|\n" +
                        "+--------------------+--------------------+\n" +
                        "Total line number = 1\n",
                "AggregateQuery ResultSets:\n" +
                        "+-------------+-------------+\n" +
                        "|SUM(us.d1.s1)|SUM(us.d1.s2)|\n" +
                        "+-------------+-------------+\n" +
                        "|     499500.0|     500499.0|\n" +
                        "+-------------+-------------+\n" +
                        "Total line number = 1\n",
                "AggregateQuery ResultSets:\n" +
                        "+-------------+-------------+\n" +
                        "|AVG(us.d1.s1)|AVG(us.d1.s2)|\n" +
                        "+-------------+-------------+\n" +
                        "|        500.0|        501.0|\n" +
                        "+-------------+-------------+\n" +
                        "Total line number = 1\n",
                "AggregateQuery ResultSets:\n" +
                        "+---------------+---------------+\n" +
                        "|COUNT(us.d1.s1)|COUNT(us.d1.s2)|\n" +
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
        String statement = "SELECT %s(s1), %s(s4) FROM us.d1 WHERE time in (0, 1000) GROUP BY 100ms;";
        List<String> funcTypeList = Arrays.asList(
                "MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT"
        );
        List<String> exceptedList = Arrays.asList(
                "DownsampleQuery ResultSets:\n" +
                        "+----+-------------+-------------+\n" +
                        "|Time|MAX(us.d1.s1)|MAX(us.d1.s4)|\n" +
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
                "DownsampleQuery ResultSets:\n" +
                        "+----+-------------+-------------+\n" +
                        "|Time|MIN(us.d1.s1)|MIN(us.d1.s4)|\n" +
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
                "DownsampleQuery ResultSets:\n" +
                        "+----+---------------------+---------------------+\n" +
                        "|Time|FIRST_VALUE(us.d1.s1)|FIRST_VALUE(us.d1.s4)|\n" +
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
                "DownsampleQuery ResultSets:\n" +
                        "+----+--------------------+--------------------+\n" +
                        "|Time|LAST_VALUE(us.d1.s1)|LAST_VALUE(us.d1.s4)|\n" +
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
                "DownsampleQuery ResultSets:\n" +
                        "+----+-------------+------------------+\n" +
                        "|Time|SUM(us.d1.s1)|     SUM(us.d1.s4)|\n" +
                        "+----+-------------+------------------+\n" +
                        "|   1|       5050.0|            5060.0|\n" +
                        "| 101|      15050.0|15060.000000000022|\n" +
                        "| 201|      25050.0| 25059.99999999997|\n" +
                        "| 301|      35050.0| 35059.99999999994|\n" +
                        "| 401|      45050.0| 45059.99999999992|\n" +
                        "| 501|      55050.0| 55059.99999999991|\n" +
                        "| 601|      65050.0|  65059.9999999999|\n" +
                        "| 701|      75050.0| 75059.99999999999|\n" +
                        "| 801|      85050.0| 85060.00000000004|\n" +
                        "| 901|      94050.0|  94059.9000000001|\n" +
                        "+----+-------------+------------------+\n" +
                        "Total line number = 10\n",
                "DownsampleQuery ResultSets:\n" +
                        "+----+------------------+------------------+\n" +
                        "|Time|     AVG(us.d1.s1)|     AVG(us.d1.s4)|\n" +
                        "+----+------------------+------------------+\n" +
                        "|   1|              50.5|              50.6|\n" +
                        "| 101|150.50000000000006|150.60000000000002|\n" +
                        "| 201| 250.4999999999999|250.59999999999997|\n" +
                        "| 301|             350.5|350.60000000000014|\n" +
                        "| 401| 450.4999999999998| 450.6000000000001|\n" +
                        "| 501| 550.5000000000003| 550.6000000000001|\n" +
                        "| 601|             650.5| 650.6000000000001|\n" +
                        "| 701| 750.5000000000002| 750.6000000000004|\n" +
                        "| 801| 850.4999999999995| 850.6000000000001|\n" +
                        "| 901|             950.0| 950.1000000000009|\n" +
                        "+----+------------------+------------------+\n" +
                        "Total line number = 10\n",
                "DownsampleQuery ResultSets:\n" +
                        "+----+---------------+---------------+\n" +
                        "|Time|COUNT(us.d1.s1)|COUNT(us.d1.s4)|\n" +
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
    public void testDelete() {
        String delete = "DELETE FROM us.d1.s1 WHERE time in (105, 115);";
        execute(delete);

        String queryOverDeleteRange = "SELECT s1 FROM us.d1 WHERE time in (100, 120);";
        String excepted = "SimpleQuery ResultSets:\n" +
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
    }

    @Test
    public void testClearData() {
        String clearData = "CLEAR DATA;";
        execute(clearData);

        String countPoints = "COUNT POINTS;";
        String excepted = "Points num: 0\n";
        executeAndCompare(countPoints, excepted);

        String showTimeSeries = "SHOW TIME SERIES;";
        excepted = "Time series:\n" +
                "+----+--------+\n" +
                "|Path|DataType|\n" +
                "+----+--------+\n" +
                "+----+--------+\n" +
                "Empty set.\n";
        executeAndCompare(showTimeSeries, excepted);
    }
}
