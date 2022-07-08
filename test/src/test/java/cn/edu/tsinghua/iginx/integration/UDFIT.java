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

public class UDFIT {

    private static final Logger logger = LoggerFactory.getLogger(UDFIT.class);

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
    public void testCOS() {
        String statement = "SELECT COS(s1) FROM us.d1 WHERE s1 < 10;";
        String expected = "ResultSets:\n" +
            "+----+--------------------+\n" +
            "|Time|       cos(us.d1.s1)|\n" +
            "+----+--------------------+\n" +
            "|   0|                 1.0|\n" +
            "|   1|  0.5403023058681398|\n" +
            "|   2| -0.4161468365471424|\n" +
            "|   3| -0.9899924966004454|\n" +
            "|   4| -0.6536436208636119|\n" +
            "|   5|  0.2836621854632263|\n" +
            "|   6|  0.9601702866503661|\n" +
            "|   7|  0.7539022543433046|\n" +
            "|   8|-0.14550003380861354|\n" +
            "|   9| -0.9111302618846769|\n" +
            "+----+--------------------+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);

        statement = "SELECT COS(s1), COS(s2) FROM us.d1 WHERE s1 < 10;";
        expected = "ResultSets:\n" +
            "+----+--------------------+--------------------+\n" +
            "|Time|       cos(us.d1.s1)|       cos(us.d1.s2)|\n" +
            "+----+--------------------+--------------------+\n" +
            "|   0|                 1.0|  0.5403023058681398|\n" +
            "|   1|  0.5403023058681398| -0.4161468365471424|\n" +
            "|   2| -0.4161468365471424| -0.9899924966004454|\n" +
            "|   3| -0.9899924966004454| -0.6536436208636119|\n" +
            "|   4| -0.6536436208636119|  0.2836621854632263|\n" +
            "|   5|  0.2836621854632263|  0.9601702866503661|\n" +
            "|   6|  0.9601702866503661|  0.7539022543433046|\n" +
            "|   7|  0.7539022543433046|-0.14550003380861354|\n" +
            "|   8|-0.14550003380861354| -0.9111302618846769|\n" +
            "|   9| -0.9111302618846769| -0.8390715290764524|\n" +
            "+----+--------------------+--------------------+\n" +
            "Total line number = 10\n";
        executeAndCompare(statement, expected);
    }
}
