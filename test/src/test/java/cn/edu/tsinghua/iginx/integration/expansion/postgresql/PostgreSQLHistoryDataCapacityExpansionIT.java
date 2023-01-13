package cn.edu.tsinghua.iginx.integration.expansion.postgresql

import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.SQLSessionIT;
import cn.edu.tsinghua.iginx.integration.expansion.BaseCapacityExpansionIT;
import cn.edu.tsinghua.iginx.integration.expansion.unit.SQLTestTools;
import cn.edu.tsinghua.iginx.session.Session;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSQLHistoryDataCapacityExpansionIT implements BaseCapacityExpansionIT {

    private static final Logger logger = LoggerFactory.getLogger(SQLSessionIT.class);

    private static Connection connection;

    private String ENGINE_TYPE;

    public PostgreSQLHistoryDataCapacityExpansionIT(String engineType) {
        this.ENGINE_TYPE = engineType;
    }

    @BeforeClass
    public static void setUp() {
//        session = new Session("127.0.0.1", 6888, "root", "root");
        String connUrl = String
                .format("jdbc:postgresql://%s:%s/?user=postgres&password=postgres", meta.getIp(), meta.getPort());
//        Connection connection = DriverManager.getConnection(connUrl);
//        Statement stmt = connection.createStatement();
        try {
//            session.openSession();
            Connection connection = DriverManager.getConnection(connUrl);
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    private static void test_compare(Connection connection,String statement,String expect){
        String connUrl = String
                .format("jdbc:postgresql://%s:%s/?user=postgres&password=postgres", meta.getIp(), meta.getPort());
        connection = DriverManager.getConnection(connUrl);
        Statement stmt = connection.createStatement();
        ResultSet rs=stmt.execute(expect);
        boolean act_true=true;
        while(rs.next()) {
            act_true=false;
            String real=rs.getString("2");
            if (expect.contains(real)){
                act_true=true;
            }
            else{
                break;
            }
        }
        if(act_true){
            logger.info("testQueryHistoryDataFromInitialNode is ok!")
        }
        else{
            logger.info("testQueryHistoryDataFromInitialNode have some problems!")
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            connection.close();
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void oriHasDataExpHasData() throws Exception {
        testQueryHistoryDataFromInitialNode();
        testQueryAfterInsertNewData();
        testCapacityExpansion_oriHasDataExpHasData();
        testWriteAndQueryAfterCapacityExpansion_oriHasDataExpHasData();
    }

    @Test
    public void oriHasDataExpNoData() throws Exception {
        testQueryHistoryDataFromInitialNode();
        testQueryAfterInsertNewData();
        testCapacityExpansion_oriHasDataExpNoData();
        testWriteAndQueryAfterCapacityExpansion_oriHasDataExpNoData();
    }

    @Test
    public void oriNoDataExpHasData() throws Exception {
        testQueryHistoryDataFromNoInitialNode();
        testQueryAfterInsertNewDataFromNoInitialNode();
        testCapacityExpansion_oriNoDataExpHasData();
        testWriteAndQueryAfterCapacityExpansion_oriNoDataExpHasData();
    }

    @Test
    public void oriNoDataExpNoData() throws Exception {
        testQueryHistoryDataFromNoInitialNode();
        testQueryAfterInsertNewDataFromNoInitialNode();
        testCapacityExpansion_oriNoDataExpNoData();
        testWriteAndQueryAfterCapacityExpansion_oriNoDataExpNoData();
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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 3\n";
        test_compare(connection, statement, expect);

        statement = "select count(*) from ln.wf01";
        expect = "ResultSets:\n" +
                "+--------------------------+-------------------------------+\n" +
                "|count(ln.wf01.wt01.status)|count(ln.wf01.wt01.temperature)|\n" +
                "+--------------------------+-------------------------------+\n" +
                "|                         2|                              1|\n" +
                "+--------------------------+-------------------------------+\n" +
                "Total line number = 1\n";
        test_compare(connection, statement, expect);

    }

    public void testQueryHistoryDataFromNoInitialNode() throws Exception {
        String statement = "select * from ln";
        String expect = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";
        test_compare(connection, statement, expect);


        statement = "count points";
        expect = "Points num: 0\n";
        test_compare(connection, statement, expect);
    }

    //@Test
    public void testQueryAfterInsertNewData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("insert into ln.wf02 (time, status, version) values (100, true, \"v1\");");
        stmt.execute("insert into ln.wf02 (time, status, version) values (400, false, \"v4\");");
        stmt.execute("insert into ln.wf02 (time, version) values (800, \"v8\");");

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 8\n";
        test_compare(connection, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     3|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        test_compare(connection, statement, expect);
    }

    public void testQueryAfterInsertNewDataFromNoInitialNode() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("insert into ln.wf02 (time, status, version) values (100, true, \"v1\");");
        stmt.execute("insert into ln.wf02 (time, status, version) values (400, false, \"v4\");");
        stmt.execute("insert into ln.wf02 (time, version) values (800, \"v8\");");

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 5\n";
        test_compare(connection, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     3|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        test_compare(connection, statement, expect);
    }

    //@Test
    public void testCapacityExpansion_oriHasDataExpNoData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:no, is_read_only:true\");");

        String statement = "select * from ln.wf03";
        String expect = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";
        test_compare(connection, statement, expect);

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 8\n";
        test_compare(connection, statement, expect);

    }

    public void testCapacityExpansion_oriHasDataExpHasData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");

        String statement = "select * from ln.wf03";
        String expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+\n" +
                "|Time|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n" +
                "+----+-------------------+------------------------+\n" +
                "|  77|               true|                    null|\n" +
                "| 200|              false|                   77.71|\n" +
                "+----+-------------------+------------------------+\n" +
                "Total line number = 2\n";
        test_compare(connection, statement, expect);

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 11\n";
        test_compare(connection, statement, expect);

    }

    public void testCapacityExpansion_oriNoDataExpHasData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:true, is_read_only:true\");");

        String statement = "select * from ln.wf03";
        String expect = "ResultSets:\n" +
                "+----+-------------------+------------------------+\n" +
                "|Time|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n" +
                "+----+-------------------+------------------------+\n" +
                "|  77|               true|                    null|\n" +
                "| 200|              false|                   77.71|\n" +
                "+----+-------------------+------------------------+\n" +
                "Total line number = 2\n";
        test_compare(connection, statement, expect);

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 8\n";
        test_compare(connection, statement, expect);

    }

    public void testCapacityExpansion_oriNoDataExpNoData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("ADD STORAGEENGINE (\"127.0.0.1\", 6668, \"" + ENGINE_TYPE + "\", \"username:root, password:root, sessionPoolSize:20, has_data:false, is_read_only:true\");");

        String statement = "select * from ln.wf03";
        String expect = "ResultSets:\n" +
                "+----+\n" +
                "|Time|\n" +
                "+----+\n" +
                "+----+\n" +
                "Empty set.\n";
        test_compare(connection, statement, expect);

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 5\n";
        test_compare(connection, statement, expect);

    }

    //@Test
    public void testWriteAndQueryAfterCapacityExpansion_oriHasDataExpHasData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("insert into ln.wf02 (time, version) values (1600, \"v48\");");

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 12\n";
        test_compare(connection, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     4|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        test_compare(connection, statement, expect);
    }

    public void testWriteAndQueryAfterCapacityExpansion_oriNoDataExpHasData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("insert into ln.wf02 (time, version) values (1600, \"v48\");");

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 9\n";
        test_compare(connection, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     4|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        test_compare(connection, statement, expect);
    }

    public void testWriteAndQueryAfterCapacityExpansion_oriHasDataExpNoData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("insert into ln.wf02 (time, version) values (1600, \"v48\");");

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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 9\n";
        test_compare(connection, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     4|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        test_compare(connection, statement, expect);
    }

    public void testWriteAndQueryAfterCapacityExpansion_oriNoDataExpNoData() throws Exception {
        Statement stmt = connection.createStatement();
        stmt.execute("insert into ln.wf02 (time, version) values (1600, \"v48\");");

        String statement = "select * from *";
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
        test_compare(connection, statement, expect);

        statement = "count points";
        expect = "Points num: 6\n";
        test_compare(connection, statement, expect);
        statement = "select count(*) from ln.wf02";
        expect = "ResultSets:\n" +
                "+---------------------+----------------------+\n" +
                "|count(ln.wf02.status)|count(ln.wf02.version)|\n" +
                "+---------------------+----------------------+\n" +
                "|                    2|                     4|\n" +
                "+---------------------+----------------------+\n" +
                "Total line number = 1\n";
        test_compare(connection, statement, expect);
    }
}
