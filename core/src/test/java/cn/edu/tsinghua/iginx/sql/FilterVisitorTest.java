package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import org.junit.Test;

public class FilterVisitorTest {

    @Test
    public void testVisit() {
        FilterVisitor visitor = new NaiveVisitor();

        String select = "SELECT a FROM root.v.c WHERE a > 5 AND b <= 10 OR c > 7 AND d == 8;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(select);
        Filter filter = statement.getFilter();
        filter.accept(visitor);

        System.out.println();

        select = "SELECT a FROM root WHERE !(a > 5 AND b <= 10 or time > 7 AND d == 8);";
        statement = (SelectStatement) TestUtils.buildStatement(select);
        filter = statement.getFilter();
        filter.accept(visitor);
    }

    static class NaiveVisitor implements FilterVisitor {

        @Override
        public void visit(AndFilter filter) {
            System.out.printf("this is and filter with %s children\n", filter.getChildren().size());
        }

        @Override
        public void visit(OrFilter filter) {
            System.out.printf("this is or filter with %s children\n", filter.getChildren().size());
        }

        @Override
        public void visit(NotFilter filter) {
            System.out.printf("this is not filter with a %s child\n", filter.getChild().getType());
        }

        @Override
        public void visit(KeyFilter filter) {
            System.out.printf("this is time filter: [%s]\n", filter.toString());
        }

        @Override
        public void visit(ValueFilter filter) {
            System.out.printf("this is value filter: [%s]\n", filter.toString());
        }

        @Override
        public void visit(PathFilter filter) {
            System.out.printf("this is path filter: [%s]\n", filter.toString());
        }

        @Override
        public void visit(BoolFilter filter) {
            System.out.printf("this is bool filter: [%s]\n", filter.isTrue());
        }
    }
}
