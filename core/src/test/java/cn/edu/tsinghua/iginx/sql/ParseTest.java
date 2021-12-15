package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    public void testParseFloatAndInteger() {
        String floatAndIntegerStr = "INSERT INTO us.d1 (timestamp, s1, s2) values (1627464728862, 10i, 1.1f), (1627464728863, 11i, 1.2f)";
        InsertStatement statement = (InsertStatement) TestUtils.buildStatement(floatAndIntegerStr);
        assertEquals("us.d1", statement.getPrefixPath());

        List<String> paths = Arrays.asList("us.d1.s1", "us.d1.s2");
        assertEquals(paths, statement.getPaths());

        assertEquals(2, statement.getTimes().size());

        List<DataType> types = Arrays.asList(DataType.INTEGER, DataType.FLOAT);
        assertEquals(types, statement.getTypes());

        Object[] s1Values = {new Integer(10), new Integer(11)};
        Object[] s2Values = {new Float(1.1), new Float(1.2)};
        assertEquals(s1Values, (Object[]) statement.getValues()[0]);
        assertEquals(s2Values, (Object[]) statement.getValues()[1]);
    }

    @Test
    public void testParseSelect() {
        String selectStr = "SELECT MAX(c), MAX(d), MAX(e), MAX(f), MIN(g) FROM a.b WHERE 100 < time and time < 1000 or d == \"abc\" or \"666\" <= c or (e < 10 and not (f < 10)) GROUP BY 1000ms;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);

        assertTrue(statement.hasFunc());
        assertTrue(statement.hasValueFilter());
        assertTrue(statement.hasGroupBy());
        assertEquals(SelectStatement.QueryType.MixedQuery, statement.getQueryType());

        assertEquals(2, statement.getSelectedFuncsAndPaths().size());
        assertTrue(statement.getSelectedFuncsAndPaths().containsKey("max"));
        assertTrue(statement.getSelectedFuncsAndPaths().containsKey("min"));

        assertEquals("a.b.c", statement.getSelectedFuncsAndPaths().get("max").get(0));
        assertEquals("a.b.d", statement.getSelectedFuncsAndPaths().get("max").get(1));
        assertEquals("a.b.e", statement.getSelectedFuncsAndPaths().get("max").get(2));
        assertEquals("a.b.f", statement.getSelectedFuncsAndPaths().get("max").get(3));
        assertEquals("a.b.g", statement.getSelectedFuncsAndPaths().get("min").get(0));

        assertEquals("a.b", statement.getFromPath());

        assertEquals("((time > 100 && time < 1000) || (a.b.d == abc) || (a.b.c >= 666) || (((a.b.e < 10 && !((a.b.f < 10))))))", statement.getFilter().toString());

        assertEquals(1000L, statement.getPrecision());
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

        String orderByAndLimit = "SELECT a FROM test ORDER BY a DESC LIMIT 10 OFFSET 5;";
        statement = (SelectStatement) TestUtils.buildStatement(orderByAndLimit);
        assertEquals("test.a", statement.getOrderByPath());
        assertFalse(statement.isAscending());
        assertEquals(5, statement.getOffset());
        assertEquals(10, statement.getLimit());

        String groupBy = "SELECT max(a) FROM test GROUP BY 5ms";
        statement = (SelectStatement) TestUtils.buildStatement(groupBy);
        assertEquals(5L, statement.getPrecision());

        String groupByAndLimit = "SELECT max(a) FROM test GROUP BY 10ms LIMIT 5 OFFSET 2;";
        statement = (SelectStatement) TestUtils.buildStatement(groupByAndLimit);
        assertEquals(10L, statement.getPrecision());
        assertEquals(2, statement.getOffset());
        assertEquals(5, statement.getLimit());
    }

    @Test(expected = SQLParserException.class)
    public void testAggregateAndOrderBy() {
        String aggregateAndOrderBy = "SELECT max(a) FROM test ORDER BY a DESC;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(aggregateAndOrderBy);
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
    public void testParseShowReplication() {
        String showReplicationStr = "SHOW REPLICA NUMBER";
        ShowReplicationStatement statement = (ShowReplicationStatement) TestUtils.buildStatement(showReplicationStr);
        assertEquals(StatementType.SHOW_REPLICATION, statement.statementType);
    }

    @Test
    public void testParseAddStorageEngine() {
        String addStorageEngineStr = "ADD STORAGEENGINE (127.0.0.1, 6667, \"iotdb11\", \"username: root, password: root\"), (127.0.0.1, 6668, \"influxdb\", \"key1: val1, key2: val2\");";
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
