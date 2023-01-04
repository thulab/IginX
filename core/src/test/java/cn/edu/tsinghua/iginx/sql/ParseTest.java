package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.expression.BaseExpression;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.join.JoinPart;
import cn.edu.tsinghua.iginx.sql.statement.join.JoinType;
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

        insertStr = "SELECT avg_s1 FROM (SELECT AVG(s1) AS avg_s1 FROM us.d1 GROUP [1000, 1600) BY 100ns) WHERE avg_s1 > 1200;";
        SelectStatement selectStatement = (SelectStatement) TestUtils.buildStatement(insertStr);
        System.out.println();
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
        List<Long> expectedTimes = Arrays.asList(
            1629965727000000000L,
            1629965727001000000L,
            1629965728000000000L,
            1629965728001000000L,
            1629965729000000000L,
            1629965729001000000L,
            1629965730000000000L,
            1629965730001000000L,
            1629965731000000000L,
            1629965731001000000L,
            1629965732000000000L,
            1629965732001000000L
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

    @Test
    public void testParseSelect() {
        String selectStr = "SELECT SUM(c), SUM(d), SUM(e), COUNT(f), COUNT(g) FROM a.b WHERE 100 < time and time < 1000 or d == \"abc\" or \"666\" <= c or (e < 10 and not (f < 10)) GROUP [200, 300) BY 10ns, LEVEL = 2, 3;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);

        assertTrue(statement.hasFunc());
        assertTrue(statement.hasValueFilter());
        assertTrue(statement.hasGroupByTime());
        assertEquals(SelectStatement.QueryType.DownSampleQuery, statement.getQueryType());

        assertEquals(2, statement.getBaseExpressionMap().size());
        assertTrue(statement.getBaseExpressionMap().containsKey("sum"));
        assertTrue(statement.getBaseExpressionMap().containsKey("count"));

        assertEquals("a.b.c", statement.getBaseExpressionMap().get("sum").get(0).getPathName());
        assertEquals("a.b.d", statement.getBaseExpressionMap().get("sum").get(1).getPathName());
        assertEquals("a.b.e", statement.getBaseExpressionMap().get("sum").get(2).getPathName());
        assertEquals("a.b.f", statement.getBaseExpressionMap().get("count").get(0).getPathName());
        assertEquals("a.b.g", statement.getBaseExpressionMap().get("count").get(1).getPathName());

        assertEquals("(((time > 100 && time < 1000) || a.b.d == \"abc\" || a.b.c >= \"666\" || (a.b.e < 10 && !a.b.f < 10)) && time >= 200 && time < 300)", statement.getFilter().toString());

        assertEquals(200, statement.getStartTime());
        assertEquals(300, statement.getEndTime());
        assertEquals(10, statement.getPrecision());

        assertEquals(Arrays.asList(2, 3), statement.getLayers());
    }

    @Test
    public void testFilter() {
        String selectStr = "SELECT a FROM root WHERE a > 100;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Collections.singletonList("root.a")), statement.getPathSet());
        assertEquals("root.a > 100", statement.getFilter().toString());

        selectStr = "SELECT a, b FROM root WHERE a > b;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Arrays.asList("root.a", "root.b")), statement.getPathSet());
        assertEquals("root.a > root.b", statement.getFilter().toString());
    }

    @Test
    public void testParseGroupBy() {
        String selectStr = "SELECT MAX(c) FROM a.b GROUP [100, 1000) BY 10ns;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(100, statement.getStartTime());
        assertEquals(1000, statement.getEndTime());
        assertEquals(10L, statement.getPrecision());

        selectStr = "SELECT SUM(c) FROM a.b GROUP BY LEVEL = 1, 2;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals("a.b.c", statement.getBaseExpressionMap().get("sum").get(0).getPathName());
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
        assertEquals(SQLConstant.KEY, statement.getOrderByPath());
        assertTrue(statement.isAscending());

        String orderByAndLimit = "SELECT a FROM test ORDER BY test.a DESC LIMIT 10 OFFSET 5;";
        statement = (SelectStatement) TestUtils.buildStatement(orderByAndLimit);
        assertEquals("test.a", statement.getOrderByPath());
        assertFalse(statement.isAscending());
        assertEquals(5, statement.getOffset());
        assertEquals(10, statement.getLimit());

        String groupBy = "SELECT max(a) FROM test GROUP (10, 120] BY 5ns";
        statement = (SelectStatement) TestUtils.buildStatement(groupBy);

        assertEquals(11, statement.getStartTime());
        assertEquals(121, statement.getEndTime());
        assertEquals(5L, statement.getPrecision());

        String groupByAndLimit = "SELECT max(a) FROM test GROUP (10, 120) BY 10ns LIMIT 5 OFFSET 2;";
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

        BaseExpression expression = subStatement.getBaseExpressionMap().get("max").get(0);
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

    @Test
    public void testParseTimeWithUnit() {
        String insertStr = "INSERT INTO a.b (timestamp, c) values "
            + "(1, 1), "
            + "(2ns, 2), "
            + "(3us, 3), "
            + "(4ms, 4), "
            + "(5s, 5), "
            + "(2022-10-01 17:24:36, 6), "
            + "(2022-10-01 17:24:36.001, 7) "
            + "(2022-10-01T17:24:36.002, 8) "
            + "(2022.10.01 17:24:36.003, 9) "
            + "(2022.10.01T17:24:36.004, 10) "
            + "(2022/10/01 17:24:36.005, 11) "
            + "(2022/10/01T17:24:36.006, 12);";
        InsertStatement insertStatement = (InsertStatement) TestUtils.buildStatement(insertStr);

        List<Long> expectedTimes = Arrays.asList(1L, 2L, 3000L, 4000000L, 5000000000L,
            1664616276000000000L, 1664616276001000000L, 1664616276002000000L, 1664616276003000000L,
            1664616276004000000L, 1664616276005000000L, 1664616276006000000L);
        assertEquals(expectedTimes, insertStatement.getTimes());

        String queryStr = "SELECT AVG(c) FROM a.b WHERE c > 10 AND c < 1ms GROUP [1s, 2s) BY 10ns;";
        SelectStatement selectStatement = (SelectStatement) TestUtils.buildStatement(queryStr);

        assertEquals("((a.b.c > 10 && a.b.c < 1000000) && time >= 1000000000 && time < 2000000000)", selectStatement.getFilter().toString());

        assertEquals(1000000000L, selectStatement.getStartTime());
        assertEquals(2000000000L, selectStatement.getEndTime());
        assertEquals(10L, selectStatement.getPrecision());
    }

    @Test
    public void testJoin() {
        String joinStr = "SELECT * FROM cpu1, cpu2";
        SelectStatement selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals("cpu1", selectStatement.getFromPath());
        assertEquals(1, selectStatement.getJoinParts().size());

        JoinPart joinPart = new JoinPart("cpu2", JoinType.CrossJoin, null, Collections.emptyList());
        assertEquals(joinPart, selectStatement.getJoinParts().get(0));

        joinStr = "SELECT * FROM cpu1, cpu2, cpu3";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals("cpu1", selectStatement.getFromPath());
        assertEquals(2, selectStatement.getJoinParts().size());

        joinPart = new JoinPart("cpu2", JoinType.CrossJoin, null, Collections.emptyList());
        assertEquals(joinPart, selectStatement.getJoinParts().get(0));

        joinPart = new JoinPart("cpu3", JoinType.CrossJoin, null, Collections.emptyList());
        assertEquals(joinPart, selectStatement.getJoinParts().get(1));

        joinStr = "SELECT * FROM cpu1 LEFT JOIN cpu2 ON cpu1.usage = cpu2.usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals("cpu1", selectStatement.getFromPath());
        assertEquals(1, selectStatement.getJoinParts().size());

        joinPart = new JoinPart("cpu2", JoinType.LeftOuterJoin,
            new PathFilter("cpu1.usage", Op.E, "cpu2.usage"), Collections.emptyList());
        assertEquals(joinPart, selectStatement.getJoinParts().get(0));

        joinStr = "SELECT * FROM cpu1 RIGHT OUTER JOIN cpu2 USING usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals("cpu1", selectStatement.getFromPath());
        assertEquals(1, selectStatement.getJoinParts().size());

        joinPart = new JoinPart("cpu2", JoinType.RightOuterJoin,
            null, Collections.singletonList("usage"));
        assertEquals(joinPart, selectStatement.getJoinParts().get(0));

        joinStr = "SELECT * FROM cpu1 FULL OUTER JOIN cpu2 ON cpu1.usage = cpu2.usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals("cpu1", selectStatement.getFromPath());
        assertEquals(1, selectStatement.getJoinParts().size());

        joinPart = new JoinPart("cpu2", JoinType.FullOuterJoin,
            new PathFilter("cpu1.usage", Op.E, "cpu2.usage"), Collections.emptyList());
        assertEquals(joinPart, selectStatement.getJoinParts().get(0));

        joinStr = "SELECT * FROM cpu1 JOIN cpu2 ON cpu1.usage = cpu2.usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals("cpu1", selectStatement.getFromPath());
        assertEquals(1, selectStatement.getJoinParts().size());

        joinPart = new JoinPart("cpu2", JoinType.InnerJoin,
            new PathFilter("cpu1.usage", Op.E, "cpu2.usage"), Collections.emptyList());
        assertEquals(joinPart, selectStatement.getJoinParts().get(0));

        joinStr = "SELECT * FROM cpu1 INNER JOIN cpu2 USING usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals("cpu1", selectStatement.getFromPath());
        assertEquals(1, selectStatement.getJoinParts().size());

        joinPart = new JoinPart("cpu2", JoinType.InnerJoin,
            null, Collections.singletonList("usage"));
        assertEquals(joinPart, selectStatement.getJoinParts().get(0));
    }
}
