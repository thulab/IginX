package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.sql.logical.ExprUtils;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import org.junit.Test;

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
}
