package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.sql.operator.AddStorageEngineOperator;
import cn.edu.tsinghua.iginx.sql.operator.DeleteOperator;
import cn.edu.tsinghua.iginx.sql.operator.InsertOperator;
import cn.edu.tsinghua.iginx.sql.operator.Operator;
import cn.edu.tsinghua.iginx.sql.operator.SelectOperator;
import cn.edu.tsinghua.iginx.sql.operator.ShowReplicationOperator;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParseTest {

    private Operator buildOperator(String sql) {
        SqlLexer lexer = new SqlLexer(CharStreams.fromString(sql));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlParser parser = new SqlParser(tokens);
        IginXSqlVisitor visitor = new IginXSqlVisitor();
        ParseTree tree = parser.sqlStatement();
        Operator operator = visitor.visit(tree);
        return operator;
    }

    @Test
    public void testParseInsert() {
        String insertStr = "INSERT INTO a.b.c (timestamp, status, hardware, num) values (1, NaN, Null, 1627399423055), (2, false, \"v2\", 1627399423056);";
        InsertOperator op = (InsertOperator) buildOperator(insertStr);
        assertEquals("a.b.c", op.getPrefixPath());

        List<String> paths = Arrays.asList("a.b.c.status", "a.b.c.hardware", "a.b.c.num");
        assertEquals(paths, op.getPaths());

        assertEquals(2, op.getTimes().length);
    }

    @Test
    public void testParseFloatAndInteger() {
        String floatAndIntegerStr = "INSERT INTO us.d1 (timestamp, s1, s2) values (1627464728862, 10i, 1.1f), (1627464728863, 11i, 1.2f)";
        InsertOperator op = (InsertOperator) buildOperator(floatAndIntegerStr);
        assertEquals("us.d1", op.getPrefixPath());

        List<String> paths = Arrays.asList("us.d1.s1", "us.d1.s2");
        assertEquals(paths, op.getPaths());

        assertEquals(2, op.getTimes().length);

        List<DataType> types = Arrays.asList(DataType.INTEGER, DataType.FLOAT);
        assertEquals(types, op.getTypes());

        Object[] s1Values = {new Integer(10), new Integer(11)};
        Object[] s2Values = {new Float(1.1), new Float(1.2)};
        assertEquals(s1Values, (Object[]) op.getValues()[0]);
        assertEquals(s2Values, (Object[]) op.getValues()[1]);
    }

    @Test
    public void testParseSelect() {
        String selectStr = "SELECT MAX(c), MAX(d), MAX(e), MAX(f) FROM a.b WHERE time in [2022-12-12 16:18:23-1s, 2022-12-12 16:18:23+1s) and d == \"abc\" or c >= \"666\" or (e < 10 and not (f < 10)) GROUP BY 1000ms;";
        SelectOperator op = (SelectOperator) buildOperator(selectStr);

        assertTrue(op.isHasFunc());
        assertTrue(op.isHasValueFilter());
        assertTrue(op.isHasGroupBy());
        assertEquals(SelectOperator.QueryType.MixedQuery, op.getQueryType());

        assertEquals(4, op.getSelectedFuncsAndPaths().size());
        assertEquals(SelectOperator.FuncType.Max.toString().toLowerCase(), op.getSelectedFuncsAndPaths().get(0).k.toLowerCase());

        assertEquals("a.b.c", op.getSelectedFuncsAndPaths().get(0).v);
        assertEquals("a.b.d", op.getSelectedFuncsAndPaths().get(1).v);
        assertEquals("a.b.e", op.getSelectedFuncsAndPaths().get(2).v);
        assertEquals("a.b.f", op.getSelectedFuncsAndPaths().get(3).v);

        assertEquals("a.b", op.getFromPath());

        assertEquals("a.b.d == \"abc\" || a.b.c >= \"666\" || !(a.b.e < 10 && !(a.b.f < 10))", op.getBooleanExpression());

        assertEquals(1670833102000l, op.getStartTime());
        assertEquals(1670833104000l, op.getEndTime());

        assertEquals(1000l, op.getPrecision());
    }

    @Test
    public void testParseSpecialClause() {
        String limit = "SELECT a FROM test LIMIT 2, 5;";
        SelectOperator op = (SelectOperator) buildOperator(limit);
        assertEquals(5, op.getLimit());
        assertEquals(2, op.getOffset());

        String orderBy = "SELECT a FROM test ORDER BY timestamp";
        op = (SelectOperator) buildOperator(orderBy);
        assertEquals(SQLConstant.TIME, op.getOrderByPath());
        assertTrue(op.isAscending());

        String orderByAndLimit = "SELECT a FROM test ORDER BY a DESC LIMIT 10 OFFSET 5;";
        op = (SelectOperator) buildOperator(orderByAndLimit);
        assertEquals("test.a", op.getOrderByPath());
        assertFalse(op.isAscending());
        assertEquals(5, op.getOffset());
        assertEquals(10, op.getLimit());

        String groupBy = "SELECT max(a) FROM test GROUP BY 5ms";
        op = (SelectOperator) buildOperator(groupBy);
        assertEquals(5L, op.getPrecision());

        String groupByAndLimit = "SELECT max(a) FROM test GROUP BY 10ms LIMIT 5 OFFSET 2;";
        op = (SelectOperator) buildOperator(groupByAndLimit);
        assertEquals(10L, op.getPrecision());
        assertEquals(2, op.getOffset());
        assertEquals(5, op.getLimit());
    }

    @Test(expected = SQLParserException.class)
    public void testAggregateAndOrderBy() {
        String aggregateAndOrderBy = "SELECT max(a) FROM test ORDER BY a DESC;";
        SelectOperator op = (SelectOperator) buildOperator(aggregateAndOrderBy);
    }

    @Test
    public void testParseDelete() {
        String deleteStr = "DELETE FROM a.b.c, a.b.d WHERE time in [1627464728862, 2022-12-12 16:18:23+1s);";
        DeleteOperator op = (DeleteOperator) buildOperator(deleteStr);
        List<String> paths = Arrays.asList("a.b.c", "a.b.d");
        assertEquals(paths, op.getPaths());

        assertEquals(1627464728862L, op.getStartTime());
        assertEquals(1670833104000L, op.getEndTime());
    }

    @Test
    public void testTimeRange() {
        String lsrs = "SELECT a FROM b WHERE TIME IN [10, 15]"; // []
        String lrrr = "SELECT a FROM b WHERE TIME IN (10, 15)"; // ()
        String lsrr = "SELECT a FROM b WHERE TIME IN [10, 15)"; // [)
        String lrrs = "SELECT a FROM b WHERE TIME IN (10, 15]"; // (]

        // [10, 15] -> [10, 16)
        SelectOperator op = (SelectOperator) buildOperator(lsrs);
        assertEquals(10, op.getStartTime());
        assertEquals(16, op.getEndTime());

        // (10, 15) -> [11, 15)
        op = (SelectOperator) buildOperator(lrrr);
        assertEquals(11, op.getStartTime());
        assertEquals(15, op.getEndTime());

        // [10, 15) -> [10, 15)
        op = (SelectOperator) buildOperator(lsrr);
        assertEquals(10, op.getStartTime());
        assertEquals(15, op.getEndTime());

        // (10, 15] -> [11, 16)
        op = (SelectOperator) buildOperator(lrrs);
        assertEquals(11, op.getStartTime());
        assertEquals(16, op.getEndTime());
    }

    @Test
    public void testParseLimitClause() {
        String selectWithLimit = "SELECT * FROM a.b LIMIT 10";
        String selectWithLimitAndOffset01 = "SELECT * FROM a.b LIMIT 2, 10";
        String selectWithLimitAndOffset02 = "SELECT * FROM a.b LIMIT 10 OFFSET 2";
        String selectWithLimitAndOffset03 = "SELECT * FROM a.b OFFSET 2 LIMIT 10";

        SelectOperator op = (SelectOperator) buildOperator(selectWithLimit);
        assertEquals(10, op.getLimit());
        assertEquals(0, op.getOffset());

        op = (SelectOperator) buildOperator(selectWithLimitAndOffset01);
        assertEquals(10, op.getLimit());
        assertEquals(2, op.getOffset());

        op = (SelectOperator) buildOperator(selectWithLimitAndOffset02);
        assertEquals(10, op.getLimit());
        assertEquals(2, op.getOffset());

        op = (SelectOperator) buildOperator(selectWithLimitAndOffset03);
        assertEquals(10, op.getLimit());
        assertEquals(2, op.getOffset());
    }

    @Test
    public void testParseShowReplication() {
        String showReplicationStr = "SHOW REPLICA NUMBER";
        ShowReplicationOperator op = (ShowReplicationOperator) buildOperator(showReplicationStr);
        assertEquals(Operator.OperatorType.SHOW_REPLICATION, op.operatorType);
    }

    @Test
    public void testParseAddStorageEngine() {
        String addStorageEngineStr = "ADD STORAGEENGINE (127.0.0.1, 6667, \"iotdb11\", \"username: root, password: root\"), (127.0.0.1, 6668, \"influxdb\", \"key1: val1, key2: val2\");";
        AddStorageEngineOperator op = (AddStorageEngineOperator) buildOperator(addStorageEngineStr);

        assertEquals(2, op.getEngines().size());

        Map<String, String> extra01 = new HashMap<>();
        extra01.put("username", "root");
        extra01.put("password", "root");
        StorageEngine engine01 = new StorageEngine("127.0.0.1", 6667, "iotdb11", extra01);

        Map<String, String> extra02 = new HashMap<>();
        extra02.put("key1", "val1");
        extra02.put("key2", "val2");
        StorageEngine engine02 = new StorageEngine("127.0.0.1", 6668, "influxdb", extra02);

        assertEquals(engine01, op.getEngines().get(0));
        assertEquals(engine02, op.getEngines().get(1));
    }
}
