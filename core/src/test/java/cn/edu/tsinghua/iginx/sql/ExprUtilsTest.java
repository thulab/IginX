package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.logical.utils.ExprUtils;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ExprUtilsTest {
    @Test
    public void testRemoveNot() {
        String select = "SELECT a FROM root WHERE !(a != 10);";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(select);
        Filter filter = statement.getFilter();
        System.out.println(filter.toString());
        System.out.println(ExprUtils.removeNot(filter).toString());

        select = "SELECT a FROM root WHERE !(!(a != 10));";
        statement = (SelectStatement) TestUtils.buildStatement(select);
        filter = statement.getFilter();
        System.out.println(filter.toString());
        System.out.println(ExprUtils.removeNot(filter).toString());

        select = "SELECT a FROM root WHERE !(a > 5 AND b <= 10 AND c > 7 AND d == 8);";
        statement = (SelectStatement) TestUtils.buildStatement(select);
        filter = statement.getFilter();
        System.out.println(filter.toString());
        System.out.println(ExprUtils.removeNot(filter).toString());

        select = "SELECT a FROM root WHERE !(a > 5 AND b <= 10 or c > 7 AND d == 8);";
        statement = (SelectStatement) TestUtils.buildStatement(select);
        filter = statement.getFilter();
        System.out.println(filter.toString());
        System.out.println(ExprUtils.removeNot(filter).toString());
    }

    @Test
    public void testToDNF() {
        String select = "SELECT a FROM root WHERE a > 5 AND b <= 10 OR c > 7 AND d == 8;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(select);
        Filter filter = statement.getFilter();
        System.out.println(filter.toString());
        System.out.println(ExprUtils.toDNF(filter).toString());

        select = "SELECT a FROM root WHERE (a > 5 OR b <= 10) AND (c > 7 OR d == 8);";
        statement = (SelectStatement) TestUtils.buildStatement(select);
        filter = statement.getFilter();
        System.out.println(filter.toString());
        System.out.println(ExprUtils.toDNF(filter).toString());

        select = "SELECT a FROM root WHERE (a > 5 OR b <= 10) AND (c > 7 OR d == 8) AND (e < 3 OR f != 2);";
        statement = (SelectStatement) TestUtils.buildStatement(select);
        filter = statement.getFilter();
        System.out.println(filter.toString());
        System.out.println(ExprUtils.toDNF(filter).toString());

        select = "SELECT a FROM root WHERE (a > 5 AND b <= 10) AND (c > 7 OR d == 8);";
        statement = (SelectStatement) TestUtils.buildStatement(select);
        filter = statement.getFilter();
        System.out.println(filter.toString());
        System.out.println(ExprUtils.toDNF(filter).toString());
    }

    @Test
    public void testTimeRange() {
        String delete = "DELETE FROM root.a WHERE (time > 5 AND time <= 10) OR (time > 12 AND time < 15);";
        DeleteStatement statement = (DeleteStatement) TestUtils.buildStatement(delete);
        assertEquals(
                Arrays.asList(
                        new TimeRange(6, 11),
                        new TimeRange(13, 15)
                ),
                statement.getTimeRanges()
        );

        delete = "DELETE FROM root.a WHERE (time > 1 AND time <= 8) OR (time >= 5 AND time < 11) OR time >= 66;";
        statement = (DeleteStatement) TestUtils.buildStatement(delete);
        assertEquals(
                Arrays.asList(
                        new TimeRange(2, 11),
                        new TimeRange(66, Long.MAX_VALUE)
                ),
                statement.getTimeRanges()
        );

        delete = "DELETE FROM root.a WHERE time >= 16 AND time < 61;";
        statement = (DeleteStatement) TestUtils.buildStatement(delete);
        assertEquals(
                Collections.singletonList(
                        new TimeRange(16, 61)
                ),
                statement.getTimeRanges()
        );

        delete = "DELETE FROM root.a WHERE time >= 16;";
        statement = (DeleteStatement) TestUtils.buildStatement(delete);
        assertEquals(
                Collections.singletonList(
                        new TimeRange(16, Long.MAX_VALUE)
                ),
                statement.getTimeRanges()
        );

        delete = "DELETE FROM root.a WHERE time < 61;";
        statement = (DeleteStatement) TestUtils.buildStatement(delete);
        assertEquals(
                Collections.singletonList(
                        new TimeRange(0, 61)
                ),
                statement.getTimeRanges()
        );

        delete = "DELETE FROM root.a WHERE time < 61 AND time > 616;";
        statement = (DeleteStatement) TestUtils.buildStatement(delete);
        assertEquals(
                new ArrayList<>(),
                statement.getTimeRanges()
        );

        delete = "DELETE FROM root.a;";
        statement = (DeleteStatement) TestUtils.buildStatement(delete);
        assertEquals(
                new ArrayList<>(),
                statement.getTimeRanges()
        );
    }
}
