package cn.edu.tsinghua.iginx.integration.history;

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.SQLSessionIT;
import cn.edu.tsinghua.iginx.session.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IoTDBHistoryDataCapacityExpansionIT {

    private static final Logger logger = LoggerFactory.getLogger(SQLSessionIT.class);

    private static Session session;

    private final String ENGINE_TYPE;

    protected boolean oriHasDataExpHasData;
    protected boolean oriHasDataExpNoData;
    protected boolean oriNoDataExpHasData;
    protected boolean oriNoDataExpNoData;


    public IoTDBHistoryDataCapacityExpansionIT(String engineType) {
        this.ENGINE_TYPE = engineType;
    }

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
        if(oriHasDataExpHasData) {
            testQueryHistoryDataFromInitialNode();
            testQueryAfterInsertNewData();
            testCapacityExpansion_oriHasDataExpHasData();
            testWriteAndQueryAfterCapacityExpansion_oriHasDataExpHasData();
        } else if(oriHasDataExpNoData) {
            testQueryHistoryDataFromInitialNode();
            testQueryAfterInsertNewData();
            testCapacityExpansion_oriHasDataExpNoData();
            testWriteAndQueryAfterCapacityExpansion_oriHasDataExpNoData();
        } else if (oriNoDataExpHasData) {
            testQueryHistoryDataFromNoInitialNode();
            testQueryAfterInsertNewDataFromNoInitialNode();
            testCapacityExpansion_oriNoDataExpHasData();
            testWriteAndQueryAfterCapacityExpansion_oriNoDataExpHasData();
        } else if (oriNoDataExpNoData) {
            testQueryHistoryDataFromNoInitialNode();
            testQueryAfterInsertNewDataFromNoInitialNode();
            testCapacityExpansion_oriNoDataExpNoData();
            testWriteAndQueryAfterCapacityExpansion_oriNoDataExpNoData();
        }

    }

    //@Test
    public void testQueryHistoryDataFromInitialNode() throws Exception {
        String statement = "select * from *";
        String expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+\n" +
                "|Time|ln.wf01.wt01.status|ln.wf01.wt01.temperature|\n" +
                "+----+-------------------+------------------------+\n" +
                "| 100|               true|                    null|\n" +
                "| 200|              false|                   20.71|\n" +
                "+----+-------------------+------------------------+\n" +
                "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 3\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf01";
        expect = "ResultSets:\n" +
                "+--------------------------+-------------------------------+\n" +
                "|count(ln.wf01.wt01.status)|count(ln.wf01.wt01.temperature)|\n" +
                "+--------------------------+-------------------------------+\n" +
                "|                         2|                              1|\n" +
                "+--------------------------+-------------------------------+\n" +
                "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testQueryHistoryDataFromNoInitialNode() throws Exception {
        String statement = "select * from ln";
        String expect = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 0\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    //@Test
    public void testQueryAfterInsertNewData() throws Exception {
        session.executeSql("insert into ln.wf02 (time, status, version) values (100, true, \"v1\");");
        session.executeSql("insert into ln.wf02 (time, status, version) values (400, false, \"v4\");");
        session.executeSql("insert into ln.wf02 (time, version) values (800, \"v8\");");

        String statement = "select * from ln";
        String expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "|Time|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "| 100|               true|                    null|          true|             v1|\n" +
                "| 200|              false|                   20.71|          null|           null|\n" +
                "| 400|               null|                    null|         false|             v4|\n" +
                "| 800|               null|                    null|          null|             v8|\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "Total line number = 4\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 8\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     3|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testQueryAfterInsertNewDataFromNoInitialNode() throws Exception {
        session.executeSql("insert into ln.wf02 (time, status, version) values (100, true, \"v1\");");
        session.executeSql("insert into ln.wf02 (time, status, version) values (400, false, \"v4\");");
        session.executeSql("insert into ln.wf02 (time, version) values (800, \"v8\");");

        String statement = "select * from ln";
        String expect = "ResultSets:\n" +
                "+----+--------------+---------------+\n" +
                "|Time|ln.wf02.status|ln.wf02.version|\n" +
                "+----+--------------+---------------+\n" +
                "| 100|          true|             v1|\n" +
                "| 400|         false|             v4|\n" +
                "| 800|          null|             v8|\n" +
                "+----+--------------+---------------+\n" +
                "Total line number = 3\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 5\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     3|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    //@Test
    public void testCapacityExpansion_oriHasDataExpNoData() throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");

        String statement = "select * from ln.wf03";
        String expect = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "|Time|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "| 100|               true|                    null|          true|             v1|\n" +
                "| 200|              false|                   20.71|          null|           null|\n" +
                "| 400|               null|                    null|         false|             v4|\n" +
                "| 800|               null|                    null|          null|             v8|\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "Total line number = 4\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 8\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

    }

    public void testCapacityExpansion_oriHasDataExpHasData() throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");

        String statement = "select * from ln.wf03";
        String expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+\n" +
                "|Time|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n" +
                "+----+-------------------+------------------------+\n" +
                "|  77|               true|                    null|\n" +
                "| 200|              false|                   77.71|\n" +
                "+----+-------------------+------------------------+\n" +
                "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n" +
                "|Time|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n" +
                "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n" +
                "|  77|               null|                    null|          null|           null|               true|                    null|\n" +
                "| 100|               true|                    null|          true|             v1|               null|                    null|\n" +
                "| 200|              false|                   20.71|          null|           null|              false|                   77.71|\n" +
                "| 400|               null|                    null|         false|             v4|               null|                    null|\n" +
                "| 800|               null|                    null|          null|             v8|               null|                    null|\n" +
                "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n" +
                "Total line number = 5\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 11\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

    }

    public void testCapacityExpansion_oriNoDataExpHasData() throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");

        String statement = "select * from ln.wf03";
        String expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+\n" +
                "|Time|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n" +
                "+----+-------------------+------------------------+\n" +
                "|  77|               true|                    null|\n" +
                "| 200|              false|                   77.71|\n" +
                "+----+-------------------+------------------------+\n" +
                "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect = "ResultSets:\n" +
                "+----+--------------+---------------+-------------------+------------------------+\n" +
                "|Time|ln.wf02.status|ln.wf02.version|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n" +
                "+----+--------------+---------------+-------------------+------------------------+\n" +
                "|  77|          null|           null|               true|                    null|\n" +
                "| 100|          true|             v1|               null|                    null|\n" +
                "| 200|          null|           null|              false|                   77.71|\n" +
                "| 400|         false|             v4|               null|                    null|\n" +
                "| 800|          null|             v8|               null|                    null|\n" +
                "+----+--------------+---------------+-------------------+------------------------+\n" +
                "Total line number = 5\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 8\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

    }

    public void testCapacityExpansion_oriNoDataExpNoData() throws Exception {
        session.executeSql("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");

        String statement = "select * from ln.wf03";
        String expect = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect = "ResultSets:\n" +
                "+----+--------------+---------------+\n" +
                "|Time|ln.wf02.status|ln.wf02.version|\n" +
                "+----+--------------+---------------+\n" +
                "| 100|          true|             v1|\n" +
                "| 400|         false|             v4|\n" +
                "| 800|          null|             v8|\n" +
                "+----+--------------+---------------+\n" +
                "Total line number = 3\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 5\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

    }

    //@Test
    public void testWriteAndQueryAfterCapacityExpansion_oriHasDataExpHasData() throws Exception {
        session.executeSql("insert into ln.wf02 (time, version) values (1600, \"v48\");");

        String statement = "select * from ln";
        String expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n" +
                "|Time|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n" +
                "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n" +
                "|  77|               null|                    null|          null|           null|               true|                    null|\n" +
                "| 100|               true|                    null|          true|             v1|               null|                    null|\n" +
                "| 200|              false|                   20.71|          null|           null|              false|                   77.71|\n" +
                "| 400|               null|                    null|         false|             v4|               null|                    null|\n" +
                "| 800|               null|                    null|          null|             v8|               null|                    null|\n" +
                "|1600|               null|                    null|          null|            v48|               null|                    null|\n" +
                "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n" +
                "Total line number = 6\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 12\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     4|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testWriteAndQueryAfterCapacityExpansion_oriNoDataExpHasData() throws Exception {
        session.executeSql("insert into ln.wf02 (time, version) values (1600, \"v48\");");

        String statement = "select * from ln";
        String expect = "ResultSets:\n" +
                "+----+--------------+---------------+-------------------+------------------------+\n" +
                "|Time|ln.wf02.status|ln.wf02.version|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n" +
                "+----+--------------+---------------+-------------------+------------------------+\n" +
                "|  77|          null|           null|               true|                    null|\n" +
                "| 100|          true|             v1|               null|                    null|\n" +
                "| 200|          null|           null|              false|                   77.71|\n" +
                "| 400|         false|             v4|               null|                    null|\n" +
                "| 800|          null|             v8|               null|                    null|\n" +
                "|1600|          null|            v48|               null|                    null|\n" +
                "+----+--------------+---------------+-------------------+------------------------+\n" +
                "Total line number = 6\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 9\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     4|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testWriteAndQueryAfterCapacityExpansion_oriHasDataExpNoData() throws Exception {
        session.executeSql("insert into ln.wf02 (time, version) values (1600, \"v48\");");

        String statement = "select * from ln";
        String expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "|Time|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "| 100|               true|                    null|          true|             v1|\n" +
                "| 200|              false|                   20.71|          null|           null|\n" +
                "| 400|               null|                    null|         false|             v4|\n" +
                "| 800|               null|                    null|          null|             v8|\n" +
                "|1600|               null|                    null|          null|            v48|\n" +
                "+----+-------------------+------------------------+--------------+---------------+\n" +
                "Total line number = 5\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 9\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     4|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testWriteAndQueryAfterCapacityExpansion_oriNoDataExpNoData() throws Exception {
        session.executeSql("insert into ln.wf02 (time, version) values (1600, \"v48\");");

        String statement = "select * from ln";
        String expect = "ResultSets:\n" +
                "+----+--------------+---------------+\n" +
                "|Time|ln.wf02.status|ln.wf02.version|\n" +
                "+----+--------------+---------------+\n" +
                "| 100|          true|             v1|\n" +
                "| 400|         false|             v4|\n" +
                "| 800|          null|             v8|\n" +
                "|1600|          null|            v48|\n" +
                "+----+--------------+---------------+\n" +
                "Total line number = 4\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "count points";
        expect = "Points num: 6\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     4|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }
}
