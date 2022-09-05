package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class ParseTest {

    @Test
    public void testParseInsert() {
        String insertStr = "INSERT INTO a.b.c (timestamp, status, hardware, num) values (1, NaN, Null, 1627399423055), (2, false, \"v2\", 1627399423056);";
        InsertStatement statement = (InsertStatement) TestUtils.buildStatement(insertStr);
        assertEquals("a.b.c", statement.getPrefixPath());

        List<String> paths = Arrays.asList("a.b.c.hardware", "a.b.c.num", "a.b.c.status");
        assertEquals(paths, statement.getPaths());

        assertEquals(2, statement.getTimes().size());
    }

    @Test
    public void testParseDateFormat() {
        String insertStr = "INSERT INTO us.d2(TIME, date) VALUES " +
            "(2021-08-26 16:15:27, 1), " +
            "(2021/08/26 16:15:28, 1), " +
            "(2021.08.26 16:15:29, 1), " +
            "(2021-08-26T16:15:30, 1), " +
            "(2021/08/26T16:15:31, 1), " +
            "(2021.08.26T16:15:32, 1), " +

            "(2021-08-26 16:15:27.001, 1), " +
            "(2021/08/26 16:15:28.001, 1), " +
            "(2021.08.26 16:15:29.001, 1), " +
            "(2021-08-26T16:15:30.001, 1), " +
            "(2021/08/26T16:15:31.001, 1), " +
            "(2021.08.26T16:15:32.001, 1);";

        InsertStatement statement = (InsertStatement) TestUtils.buildStatement(insertStr);
        statement.getTimes();
        List<Long> expectedTimes = Arrays.asList(
            1629965727000L,
            1629965727001L,
            1629965728000L,
            1629965728001L,
            1629965729000L,
            1629965729001L,
            1629965730000L,
            1629965730001L,
            1629965731000L,
            1629965731001L,
            1629965732000L,
            1629965732001L
        );
        assertEquals(expectedTimes, statement.getTimes());
    }

    @Test
    public void testParseInsertWithSubQuery() {
        String insertStr = "INSERT INTO test.copy (timestamp, status, hardware, num) values (SELECT status, hardware, num FROM test) TIME_OFFSET = 5;";
        InsertFromSelectStatement statement = (InsertFromSelectStatement) TestUtils.buildStatement(insertStr);

        InsertStatement insertStatement = statement.getSubInsertStatement();
        assertEquals("test.copy", insertStatement.getPrefixPath());

        List<String> paths = Arrays.asList("test.copy.status", "test.copy.hardware", "test.copy.num");
        assertEquals(paths, insertStatement.getPaths());

        SelectStatement selectStatement = statement.getSubSelectStatement();

        paths = Arrays.asList("test.status", "test.hardware", "test.num");
        assertEquals(paths, selectStatement.getSelectedPaths());

        assertEquals(5, statement.getTimeOffset());
    }

//    @Test
//    public void testParseFloatAndInteger() {
//        String floatAndIntegerStr = "INSERT INTO us.d1 (timestamp, s1, s2) values (1627464728862, 10i, 1.1f), (1627464728863, 11i, 1.2f)";
//        InsertStatement statement = (InsertStatement) TestUtils.buildStatement(floatAndIntegerStr);
//        assertEquals("us.d1", statement.getPrefixPath());
//
//        List<String> paths = Arrays.asList("us.d1.s1", "us.d1.s2");
//        assertEquals(paths, statement.getPaths());
//
//        assertEquals(2, statement.getTimes().size());
//
//        List<DataType> types = Arrays.asList(DataType.INTEGER, DataType.FLOAT);
//        assertEquals(types, statement.getTypes());
//
//        Object[] s1Values = {new Integer(10), new Integer(11)};
//        Object[] s2Values = {new Float(1.1), new Float(1.2)};
//        assertEquals(s1Values, (Object[]) statement.getValues()[0]);
//        assertEquals(s2Values, (Object[]) statement.getValues()[1]);
//    }

    @Test
    public void testParseSelect() {
        String selectStr = "SELECT SUM(c), SUM(d), SUM(e), COUNT(f), COUNT(g) FROM a.b WHERE 100 < time and time < 1000 or d == \"abc\" or \"666\" <= c or (e < 10 and not (f < 10)) GROUP [200, 300) BY 10ms, LEVEL = 2, 3;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);

        assertTrue(statement.hasFunc());
        assertTrue(statement.hasValueFilter());
        assertTrue(statement.hasGroupByTime());
        assertEquals(SelectStatement.QueryType.DownSampleQuery, statement.getQueryType());

        assertEquals(2, statement.getSelectedFuncsAndExpressions().size());
        assertTrue(statement.getSelectedFuncsAndExpressions().containsKey("sum"));
        assertTrue(statement.getSelectedFuncsAndExpressions().containsKey("count"));

        assertEquals("a.b.c", statement.getSelectedFuncsAndExpressions().get("sum").get(0).getPathName());
        assertEquals("a.b.d", statement.getSelectedFuncsAndExpressions().get("sum").get(1).getPathName());
        assertEquals("a.b.e", statement.getSelectedFuncsAndExpressions().get("sum").get(2).getPathName());
        assertEquals("a.b.f", statement.getSelectedFuncsAndExpressions().get("count").get(0).getPathName());
        assertEquals("a.b.g", statement.getSelectedFuncsAndExpressions().get("count").get(1).getPathName());

        assertEquals(Collections.singletonList("a.b"), statement.getFromPaths());

        assertEquals("(((time > 100 && time < 1000) || a.b.d == \"abc\" || a.b.c >= \"666\" || (a.b.e < 10 && !a.b.f < 10)) && time >= 200 && time < 300)", statement.getFilter().toString());

        assertEquals(200, statement.getStartTime());
        assertEquals(300, statement.getEndTime());
        assertEquals(10L, statement.getPrecision());

        assertEquals(Arrays.asList(2, 3), statement.getLayers());
    }

    @Test
    public void testFilter() {
        String selectStr = "SELECT a FROM root WHERE a > 100;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Collections.singletonList("root.a")), statement.getPathSet());
        assertEquals("root.a > 100", statement.getFilter().toString());

        selectStr = "SELECT a, b FROM c, d WHERE a > 10;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Arrays.asList("c.a", "c.b", "d.a", "d.b")), statement.getPathSet());
        assertEquals("(c.a > 10 && d.a > 10)", statement.getFilter().toString());

        selectStr = "SELECT a, b FROM c, d WHERE a > 10 AND b < 20;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Arrays.asList("c.a", "c.b", "d.a", "d.b")), statement.getPathSet());
        assertEquals("((c.a > 10 && d.a > 10) && (c.b < 20 && d.b < 20))", statement.getFilter().toString());

        selectStr = "SELECT a, b FROM c, d WHERE INTACT(c.a) > 10 AND INTACT(d.b) < 20;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Arrays.asList("c.a", "c.b", "d.a", "d.b")), statement.getPathSet());
        assertEquals("(c.a > 10 && d.b < 20)", statement.getFilter().toString());

        selectStr = "SELECT a, b FROM root WHERE a > b;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Arrays.asList("root.a", "root.b")), statement.getPathSet());
        assertEquals("root.a > root.b", statement.getFilter().toString());

        selectStr = "SELECT a, b FROM c, d WHERE a > b;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Arrays.asList("c.a", "c.b", "d.a", "d.b")), statement.getPathSet());
        assertEquals("(c.a > c.b && c.a > d.b && d.a > c.b && d.a > d.b)", statement.getFilter().toString());
    }

    @Test
    public void testParseGroupBy() {
        String selectStr = "SELECT MAX(c) FROM a.b GROUP [100, 1000) BY 10ms;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(100, statement.getStartTime());
        assertEquals(1000, statement.getEndTime());
        assertEquals(10L, statement.getPrecision());

        selectStr = "SELECT SUM(c) FROM a.b GROUP BY LEVEL = 1, 2;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals("a.b.c", statement.getSelectedFuncsAndExpressions().get("sum").get(0).getPathName());
        assertEquals(Arrays.asList(1, 2), statement.getLayers());
    }

    @Test
    public void testParseSpecialClause() {
        String limit = "SELECT a FROM test LIMIT 2, 5;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(limit);
        assertEquals(5, statement.getLimit());
        assertEquals(2, statement.getOffset());

        String orderBy = "SELECT a FROM test ORDER BY timestamp";
        statement = (SelectStatement) TestUtils.buildStatement(orderBy);
        assertEquals(SQLConstant.TIME, statement.getOrderByPath());
        assertTrue(statement.isAscending());

        String orderByAndLimit = "SELECT a FROM test ORDER BY test.a DESC LIMIT 10 OFFSET 5;";
        statement = (SelectStatement) TestUtils.buildStatement(orderByAndLimit);
        assertEquals("test.a", statement.getOrderByPath());
        assertFalse(statement.isAscending());
        assertEquals(5, statement.getOffset());
        assertEquals(10, statement.getLimit());

        String groupBy = "SELECT max(a) FROM test GROUP (10, 120] BY 5ms";
        statement = (SelectStatement) TestUtils.buildStatement(groupBy);

        assertEquals(11, statement.getStartTime());
        assertEquals(121, statement.getEndTime());
        assertEquals(5L, statement.getPrecision());

        String groupByAndLimit = "SELECT max(a) FROM test GROUP (10, 120) BY 10ms LIMIT 5 OFFSET 2;";
        statement = (SelectStatement) TestUtils.buildStatement(groupByAndLimit);
        assertEquals(11, statement.getStartTime());
        assertEquals(120, statement.getEndTime());
        assertEquals(10L, statement.getPrecision());
        assertEquals(2, statement.getOffset());
        assertEquals(5, statement.getLimit());
    }

    @Test(expected = SQLParserException.class)
    public void testAggregateAndOrderBy() {
        String aggregateAndOrderBy = "SELECT max(a) FROM test ORDER BY a DESC;";
        TestUtils.buildStatement(aggregateAndOrderBy);
    }

    @Test
    public void testParseDelete() {
        String deleteStr = "DELETE FROM a.b.c, a.b.d WHERE time > 1627464728862 AND time < 2022-12-12 16:18:23+1s;";
        DeleteStatement statement = (DeleteStatement) TestUtils.buildStatement(deleteStr);
        List<String> paths = Arrays.asList("a.b.c", "a.b.d");
        assertEquals(paths, statement.getPaths());
    }

    @Test
    public void testParseDeleteTimeSeries() {
        String deleteTimeSeriesStr = "DELETE TIME SERIES a.b.c, a.b.d;";
        DeleteTimeSeriesStatement statement = (DeleteTimeSeriesStatement) TestUtils.buildStatement(deleteTimeSeriesStr);
        List<String> paths = Arrays.asList("a.b.c", "a.b.d");
        assertEquals(paths, statement.getPaths());
    }

    @Test
    public void testParseLimitClause() {
        String selectWithLimit = "SELECT * FROM a.b LIMIT 10";
        String selectWithLimitAndOffset01 = "SELECT * FROM a.b LIMIT 2, 10";
        String selectWithLimitAndOffset02 = "SELECT * FROM a.b LIMIT 10 OFFSET 2";
        String selectWithLimitAndOffset03 = "SELECT * FROM a.b OFFSET 2 LIMIT 10";

        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectWithLimit);
        assertEquals(10, statement.getLimit());
        assertEquals(0, statement.getOffset());

        statement = (SelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset01);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());

        statement = (SelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset02);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());

        statement = (SelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset03);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());
    }

    @Test
    public void testSubQueryClause() {
        String selectWithSubQuery = "SELECT res.max_a FROM (SELECT max(a) AS max_a FROM root AS res);";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectWithSubQuery);
        assertEquals(Collections.singletonList("res.max_a"), statement.getSelectedPaths());

        SelectStatement subStatement = statement.getSubStatement();

        Expression expression = subStatement.getSelectedFuncsAndExpressions().get("max").get(0);
        assertEquals("root.a", expression.getPathName());
        assertEquals("max", expression.getFuncName());
        assertEquals("res.max_a", expression.getAlias());
    }

    @Test
    public void testParseShowReplication() {
        String showReplicationStr = "SHOW REPLICA NUMBER";
        ShowReplicationStatement statement = (ShowReplicationStatement) TestUtils.buildStatement(showReplicationStr);
        assertEquals(StatementType.SHOW_REPLICATION, statement.statementType);
    }

    @Test
    public void testParseAddStorageEngine() {
        String addStorageEngineStr = "ADD STORAGEENGINE (\"127.0.0.1\", 6667, \"iotdb11\", \"username: root, password: root\"), (\"127.0.0.1\", 6668, \"influxdb\", \"key1: val1, key2: val2\");";
        AddStorageEngineStatement statement = (AddStorageEngineStatement) TestUtils.buildStatement(addStorageEngineStr);

        assertEquals(2, statement.getEngines().size());

        Map<String, String> extra01 = new HashMap<>();
        extra01.put("username", "root");
        extra01.put("password", "root");
        StorageEngine engine01 = new StorageEngine("127.0.0.1", 6667, "iotdb11", extra01);

        Map<String, String> extra02 = new HashMap<>();
        extra02.put("key1", "val1");
        extra02.put("key2", "val2");
        StorageEngine engine02 = new StorageEngine("127.0.0.1", 6668, "influxdb", extra02);

        assertEquals(engine01, statement.getEngines().get(0));
        assertEquals(engine02, statement.getEngines().get(1));
    }
}
