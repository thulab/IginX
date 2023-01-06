package cn.edu.tsinghua.iginx.integration.expansion.parquet;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.unit.SQLTestTools;
import cn.edu.tsinghua.iginx.session.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetHistoryDataCapacityExpansionIT {

    private static final Logger logger = LoggerFactory.getLogger(ParquetHistoryDataCapacityExpansionIT.class);

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

    @Test
    public void test() throws Exception {
        testInitialNodeInsertAndQuery();
        testCapacityExpansion();
        testWriteAndQueryAfterCapacityExpansion();
    }

    private void testInitialNodeInsertAndQuery() throws Exception {
        session.executeSql("insert into test.us (key, cpu_usage, engine, desc) values (10, 12.1, 1, \"normal\");");
        session.executeSql("insert into test.us (key, cpu_usage, engine, desc) values (11, 32.2, 2, \"normal\");");
        session.executeSql("insert into test.us (key, cpu_usage, engine, desc) values (12, 66.8, 3, \"high\");");

        String statement = "select * from test";
        String expect =
            "ResultSets:\n"
                + "+---+-----------------+------------+--------------+\n"
                + "|key|test.us.cpu_usage|test.us.desc|test.us.engine|\n"
                + "+---+-----------------+------------+--------------+\n"
                + "| 10|             12.1|      normal|             1|\n"
                + "| 11|             32.2|      normal|             2|\n"
                + "| 12|             66.8|        high|             3|\n"
                + "+---+-----------------+------------+--------------+\n"
                + "Total line number = 3\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 9\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from test.us";
        expect =
            "ResultSets:\n"
                + "+------------------------+-------------------+---------------------+\n"
                + "|count(test.us.cpu_usage)|count(test.us.desc)|count(test.us.engine)|\n"
                + "+------------------------+-------------------+---------------------+\n"
                + "|                       3|                  3|                    3|\n"
                + "+------------------------+-------------------+---------------------+\n"
                + "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    private void testCapacityExpansion() throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6610, \"parquet\", \"dir:test, has_data:true, is_read_only:true\");");

        String statement = "select * from test.us";
        String expect =
            "ResultSets:\n"
                + "+---+-----------------+------------+--------------+\n"
                + "|key|test.us.cpu_usage|test.us.desc|test.us.engine|\n"
                + "+---+-----------------+------------+--------------+\n"
                + "| 10|             12.1|      normal|             1|\n"
                + "| 11|             32.2|      normal|             2|\n"
                + "| 12|             66.8|        high|             3|\n"
                + "+---+-----------------+------------+--------------+\n"
                + "Total line number = 3\n";;
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from test";
        expect =
            "ResultSets:\n"
                + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
                + "|key|test.cpu_usage|test.engine|test.status|test.us.cpu_usage|test.us.desc|test.us.engine|\n"
                + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
                + "|  1|          12.3|          1|     normal|             null|        null|          null|\n"
                + "|  2|          23.1|          2|     normal|             null|        null|          null|\n"
                + "| 10|          null|       null|       null|             12.1|      normal|             1|\n"
                + "| 11|          null|       null|       null|             32.2|      normal|             2|\n"
                + "| 12|          null|       null|       null|             66.8|        high|             3|\n"
                + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
                + "Total line number = 5\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 15\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    private void testWriteAndQueryAfterCapacityExpansion() throws Exception {
        session.executeSql("insert into test.us (key, cpu_usage, engine, desc) values (13, 88.8, 2, \"high\");");

        String statement = "select * from test";
        String expect =
            "ResultSets:\n"
                + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
                + "|key|test.cpu_usage|test.engine|test.status|test.us.cpu_usage|test.us.desc|test.us.engine|\n"
                + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
                + "|  1|          12.3|          1|     normal|             null|        null|          null|\n"
                + "|  2|          23.1|          2|     normal|             null|        null|          null|\n"
                + "| 10|          null|       null|       null|             12.1|      normal|             1|\n"
                + "| 11|          null|       null|       null|             32.2|      normal|             2|\n"
                + "| 12|          null|       null|       null|             66.8|        high|             3|\n"
                + "| 13|          null|       null|       null|             88.8|        high|             2|\n"
                + "+---+--------------+-----------+-----------+-----------------+------------+--------------+\n"
                + "Total line number = 6\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 18\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from test.us";
        expect =
            "ResultSets:\n"
                + "+------------------------+-------------------+---------------------+\n"
                + "|count(test.us.cpu_usage)|count(test.us.desc)|count(test.us.engine)|\n"
                + "+------------------------+-------------------+---------------------+\n"
                + "|                       4|                  4|                    4|\n"
                + "+------------------------+-------------------+---------------------+\n"
                + "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
        SQLTestTools.executeAndCompare(session, statement, expect);
    }
}
