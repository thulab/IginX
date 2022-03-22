package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParseTest {

    private Statement buildStatement(String sql) {
        SqlLexer lexer = new SqlLexer(CharStreams.fromString(sql));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlParser parser = new SqlParser(tokens);
        IginXSqlVisitor visitor = new IginXSqlVisitor();
        ParseTree tree = parser.sqlStatement();
        return visitor.visit(tree);
    }

    @Test
    public void testParseInsert() {
        String insertStr = "INSERT INTO a.b.c (timestamp, status, hardware, num) values (1, NaN, Null, 1627399423055), (2, false, \"v2\", 1627399423056);";
        InsertStatement statement = (InsertStatement) buildStatement(insertStr);
        assertEquals("a.b.c", statement.getPrefixPath());

        List<String> paths = Arrays.asList("a.b.c.status", "a.b.c.hardware", "a.b.c.num");
        assertEquals(paths, statement.getPaths());

        assertEquals(2, statement.getTimes().length);
    }

    @Test
    public void testParseFloatAndInteger() {
        String floatAndIntegerStr = "INSERT INTO us.d1 (timestamp, s1, s2) values (1627464728862, 10i, 1.1f), (1627464728863, 11i, 1.2f)";
        InsertStatement statement = (InsertStatement) buildStatement(floatAndIntegerStr);
        assertEquals("us.d1", statement.getPrefixPath());

        List<String> paths = Arrays.asList("us.d1.s1", "us.d1.s2");
        assertEquals(paths, statement.getPaths());

        assertEquals(2, statement.getTimes().length);

        List<DataType> types = Arrays.asList(DataType.INTEGER, DataType.FLOAT);
        assertEquals(types, statement.getTypes());

        Object[] s1Values = {new Integer(10), new Integer(11)};
        Object[] s2Values = {new Float(1.1), new Float(1.2)};
        assertEquals(s1Values, (Object[]) statement.getValues()[0]);
        assertEquals(s2Values, (Object[]) statement.getValues()[1]);
    }

    @Test
    public void testParseSelect() {
        String selectStr = "SELECT MAX(c), MAX(d), MAX(e), MAX(f) FROM a.b WHERE time in [2022-12-12 16:18:23-1s, 2022-12-12 16:18:23+1s) and d == \"abc\" or c >= \"666\" or (e < 10 and not (f < 10)) GROUP BY 1000ms;";
        SelectStatement statement = (SelectStatement) buildStatement(selectStr);

        assertTrue(statement.isHasFunc());
        assertTrue(statement.isHasValueFilter());
        assertTrue(statement.isHasGroupBy());
        assertEquals(SelectStatement.QueryType.MixedQuery, statement.getQueryType());

        assertEquals(4, statement.getSelectedFuncsAndPaths().size());
        assertEquals(SelectStatement.FuncType.Max.toString().toLowerCase(), statement.getSelectedFuncsAndPaths().get(0).k.toLowerCase());

        assertEquals("a.b.c", statement.getSelectedFuncsAndPaths().get(0).v);
        assertEquals("a.b.d", statement.getSelectedFuncsAndPaths().get(1).v);
        assertEquals("a.b.e", statement.getSelectedFuncsAndPaths().get(2).v);
        assertEquals("a.b.f", statement.getSelectedFuncsAndPaths().get(3).v);

        assertEquals("a.b", statement.getFromPath());

        assertEquals("a.b.d == \"abc\" || a.b.c >= \"666\" || !(a.b.e < 10 && !(a.b.f < 10))", statement.getBooleanExpression());

        assertEquals(1670833102000L, statement.getStartTime());
        assertEquals(1670833104000L, statement.getEndTime());

        assertEquals(1000L, statement.getPrecision());
    }

    @Test
    public void testParseSpecialClause() {
        String limit = "SELECT a FROM test LIMIT 2, 5;";
        SelectStatement statement = (SelectStatement) buildStatement(limit);
        assertEquals(5, statement.getLimit());
        assertEquals(2, statement.getOffset());

        String orderBy = "SELECT a FROM test ORDER BY timestamp";
        statement = (SelectStatement) buildStatement(orderBy);
        assertEquals(SQLConstant.TIME, statement.getOrderByPath());
        assertTrue(statement.isAscending());

        String orderByAndLimit = "SELECT a FROM test ORDER BY a DESC LIMIT 10 OFFSET 5;";
        statement = (SelectStatement) buildStatement(orderByAndLimit);
        assertEquals("test.a", statement.getOrderByPath());
        assertFalse(statement.isAscending());
        assertEquals(5, statement.getOffset());
        assertEquals(10, statement.getLimit());

        String groupBy = "SELECT max(a) FROM test GROUP BY 5ms";
        statement = (SelectStatement) buildStatement(groupBy);
        assertEquals(5L, statement.getPrecision());

        String groupByAndLimit = "SELECT max(a) FROM test GROUP BY 10ms LIMIT 5 OFFSET 2;";
        statement = (SelectStatement) buildStatement(groupByAndLimit);
        assertEquals(10L, statement.getPrecision());
        assertEquals(2, statement.getOffset());
        assertEquals(5, statement.getLimit());
        assertEquals(Collections.emptyList(), statement.getLayers());

        String groupByLevel = "SELECT count(a) FROM test GROUP BY LEVEL = 2,3;";
        statement = (SelectStatement) buildStatement(groupByLevel);
        assertEquals(Arrays.asList(2, 3), statement.getLayers());

        String groupByTimeAndLevel = "SELECT count(a) FROM test GROUP BY 10ms, LEVEL = 2,3;";
        statement = (SelectStatement) buildStatement(groupByTimeAndLevel);
        assertEquals(10L, statement.getPrecision());
        assertEquals(Arrays.asList(2, 3), statement.getLayers());
    }

    @Test(expected = SQLParserException.class)
    public void testAggregateAndOrderBy() {
        String aggregateAndOrderBy = "SELECT max(a) FROM test ORDER BY a DESC;";
        SelectStatement statement = (SelectStatement) buildStatement(aggregateAndOrderBy);
    }

    @Test
    public void testParseDelete() {
        String deleteStr = "DELETE FROM a.b.c, a.b.d WHERE time in [1627464728862, 2022-12-12 16:18:23+1s);";
        DeleteStatement statement = (DeleteStatement) buildStatement(deleteStr);
        List<String> paths = Arrays.asList("a.b.c", "a.b.d");
        assertEquals(paths, statement.getPaths());

        assertEquals(1627464728862L, statement.getStartTime());
        assertEquals(1670833104000L, statement.getEndTime());
    }

    @Test
    public void testParseDeleteTimeSeries() {
        String deleteTimeSeriesStr = "DELETE TIMESERIES a.b.c, a.b.d;";
        DeleteTimeSeriesStatement statement = (DeleteTimeSeriesStatement) buildStatement(deleteTimeSeriesStr);
        List<String> paths = Arrays.asList("a.b.c", "a.b.d");
        assertEquals(paths, statement.getPaths());
    }

    @Test
    public void testTimeRange() {
        String lsrs = "SELECT a FROM b WHERE TIME IN [10, 15]"; // []
        String lrrr = "SELECT a FROM b WHERE TIME IN (10, 15)"; // ()
        String lsrr = "SELECT a FROM b WHERE TIME IN [10, 15)"; // [)
        String lrrs = "SELECT a FROM b WHERE TIME IN (10, 15]"; // (]

        // [10, 15] -> [10, 16)
        SelectStatement statement = (SelectStatement) buildStatement(lsrs);
        assertEquals(10, statement.getStartTime());
        assertEquals(16, statement.getEndTime());

        // (10, 15) -> [11, 15)
        statement = (SelectStatement) buildStatement(lrrr);
        assertEquals(11, statement.getStartTime());
        assertEquals(15, statement.getEndTime());

        // [10, 15) -> [10, 15)
        statement = (SelectStatement) buildStatement(lsrr);
        assertEquals(10, statement.getStartTime());
        assertEquals(15, statement.getEndTime());

        // (10, 15] -> [11, 16)
        statement = (SelectStatement) buildStatement(lrrs);
        assertEquals(11, statement.getStartTime());
        assertEquals(16, statement.getEndTime());
    }

    @Test
    public void testParseLimitClause() {
        String selectWithLimit = "SELECT * FROM a.b LIMIT 10";
        String selectWithLimitAndOffset01 = "SELECT * FROM a.b LIMIT 2, 10";
        String selectWithLimitAndOffset02 = "SELECT * FROM a.b LIMIT 10 OFFSET 2";
        String selectWithLimitAndOffset03 = "SELECT * FROM a.b OFFSET 2 LIMIT 10";

        SelectStatement statement = (SelectStatement) buildStatement(selectWithLimit);
        assertEquals(10, statement.getLimit());
        assertEquals(0, statement.getOffset());

        statement = (SelectStatement) buildStatement(selectWithLimitAndOffset01);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());

        statement = (SelectStatement) buildStatement(selectWithLimitAndOffset02);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());

        statement = (SelectStatement) buildStatement(selectWithLimitAndOffset03);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());
    }

    @Test
    public void testParseShowReplication() {
        String showReplicationStr = "SHOW REPLICA NUMBER";
        ShowReplicationStatement statement = (ShowReplicationStatement) buildStatement(showReplicationStr);
        assertEquals(Statement.StatementType.SHOW_REPLICATION, statement.statementType);
    }

    @Test
    public void testParseAddStorageEngine() {
        String addStorageEngineStr = "ADD STORAGEENGINE (127.0.0.1, 6667, \"iotdb11\", \"username: root, password: root\"), (127.0.0.1, 6668, \"influxdb\", \"key1: val1, key2: val2\");";
        AddStorageEngineStatement statement = (AddStorageEngineStatement) buildStatement(addStorageEngineStr);

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
    public void testAuth() {
        String createUser = "CREATE USER root1 IDENTIFIED BY root1;";
        CreateUserStatement createUserStatement = (CreateUserStatement) buildStatement(createUser);
        assertEquals("root1", createUserStatement.getUsername());
        assertEquals("root1", createUserStatement.getPassword());

        createUser = "CREATE USER root2 IDENTIFIED BY root2;";
        createUserStatement = (CreateUserStatement) buildStatement(createUser);
        assertEquals("root2", createUserStatement.getUsername());
        assertEquals("root2", createUserStatement.getPassword());

        String updateUser = "GRANT WRITE, READ TO USER root1;";
        GrantUserStatement grantUserStatement = (GrantUserStatement) buildStatement(updateUser);
        assertEquals("root1", grantUserStatement.getUsername());
        assertEquals(new HashSet<>(Arrays.asList(AuthType.Read, AuthType.Write)), grantUserStatement.getAuthTypes());

        String changePassword = "SET PASSWORD FOR root1 = PASSWORD(root3);";
        ChangePasswordStatement changePasswordStatement = (ChangePasswordStatement) buildStatement(changePassword);
        assertEquals("root1", changePasswordStatement.getUsername());
        assertEquals("root3", changePasswordStatement.getPassword());

        String deleteUser = "DROP USER root1;";
        DropUserStatement dropUserStatement = (DropUserStatement) buildStatement(deleteUser);
        assertEquals("root1", dropUserStatement.getUsername());

        String showUser = "SHOW USER root1, root2;";
        ShowUserStatement showUserStatement = (ShowUserStatement) buildStatement(showUser);
        assertEquals(new ArrayList<>(Arrays.asList("root1", "root2")), showUserStatement.getUsers());
    }
}
