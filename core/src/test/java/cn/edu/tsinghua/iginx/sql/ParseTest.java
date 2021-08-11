package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.sql.operator.*;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class ParseTest {
    private static String insertStr = "INSERT INTO a.b.c (timestamp, status, hardware, number) values (1, NaN, Null, 1627399423055), (2, false, \"v2\", 1627399423056);";
    private static String deleteStr = "DELETE FROM a.b.c, a.b.d WHERE time in [1627464728862, 2022-12-12 16:18:23+1s);";
    private static String selectStr = "SELECT MAX(a.b.c) FROM a.b.c, a.b.d, a.b.e, a.b.f WHERE time in [2022-12-12 16:18:23-1s, 2022-12-12 16:18:23+1s) and d == \"abc\" or c >= \"666\" or (e < 10 and not (f < 10)) GROUP BY 1000ms;";
    private static String showReplicationStr = "SHOW REPLICATION";
    private static String addStorageEngineStr = "ADD STORAGEENGINE (127.0.0.1, 6667, IotDB, \"{clause: hello world!  }\"), (127.0.0.1, 6668, InfluxDB, \"{key: val}\");";
    private static String floatAndIntegerStr = "INSERT INTO us.d1 (timestamp, s1, s2) values (1627464728862, 10i, 1.1f), (1627464728863, 11i, 1.2f)";

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
        InsertOperator op = (InsertOperator) buildOperator(insertStr);
        assertEquals("a.b.c", op.getPrefixPath());

        List<String> paths = Arrays.asList("a.b.c.status", "a.b.c.hardware", "a.b.c.number");
        assertEquals(paths, op.getPaths());

        assertEquals(2, op.getTimes().length);
    }

    @Test
    public void testParseFloatAndInteger() {
        InsertOperator op = (InsertOperator) buildOperator(floatAndIntegerStr);
        assertEquals("us.d1", op.getPrefixPath());

        List<String> paths = Arrays.asList("us.d1.s1", "us.d1.s2");
        assertEquals(paths, op.getPaths());

        assertEquals(2, op.getTimes().length);

        List<DataType> types = Arrays.asList(DataType.INTEGER, DataType.FLOAT);
        assertEquals(types, op.getTypes());

        Object[] s1Values = {new Integer(10), new Integer(11)};
        Object[] s2Values = {new Float(1.1), new Float(1.2)};
        assertEquals(s1Values, (Object[])op.getValues()[0]);
        assertEquals(s2Values, (Object[])op.getValues()[1]);
    }

    @Test
    public void testParseSelect() {
        SelectOperator op = (SelectOperator) buildOperator(selectStr);

        assertEquals(true, op.isHasFunc());
        assertEquals(true, op.isHasValueFilter());
        assertEquals(true, op.isHasGroupBy());
        assertEquals(SelectOperator.QueryType.NotSupport, op.getQueryType());

        assertEquals(1, op.getSelectedFuncsAndPaths().size());
        assertEquals(SelectOperator.FuncType.Max, op.getSelectedFuncsAndPaths().get(0).k);
        assertEquals("a.b.c", op.getSelectedFuncsAndPaths().get(0).v);

        List<String> paths = Arrays.asList("a.b.c", "a.b.d", "a.b.e", "a.b.f");
        assertEquals(paths, op.getFromPaths());

        assertEquals("d == \"abc\" || c >= \"666\" || !(e < 10 && !(f < 10))", op.getBooleanExpression());
        assertEquals(1670833102000l, op.getStartTime());
        assertEquals(1670833104000l, op.getEndTime());

        assertEquals(1000l, op.getPrecision());
    }

    @Test
    public void testParseDelete() {
        DeleteOperator op = (DeleteOperator) buildOperator(deleteStr);
        List<String> paths = Arrays.asList("a.b.c", "a.b.d");
        assertEquals(paths, op.getPaths());

        assertEquals(1627464728862L, op.getStartTime());
        assertEquals(1670833104000L, op.getEndTime());
    }

    @Test
    public void testParseShowReplication() {
        ShowReplicationOperator op = (ShowReplicationOperator) buildOperator(showReplicationStr);
        assertEquals(Operator.OperatorType.SHOW_REPLICATION, op.operatorType);
    }

    @Test
    public void testParseAddStorageEngine() {
        AddStorageEngineOperator op = (AddStorageEngineOperator) buildOperator(addStorageEngineStr);

        assertEquals(2, op.getEngines().size());

        Map<String, String> extra01 = new HashMap<>();
        extra01.put("clause", "hello world!");
        StorageEngine engine01 = new StorageEngine("127.0.0.1", 6667, StorageEngineType.IOTDB, extra01);

        Map<String, String> extra02 = new HashMap<>();
        extra02.put("key", "val");
        StorageEngine engine02 = new StorageEngine("127.0.0.1", 6668, StorageEngineType.INFLUXDB, extra02);

        assertEquals(engine01, op.getEngines().get(0));
        assertEquals(engine02, op.getEngines().get(1));
    }
}
