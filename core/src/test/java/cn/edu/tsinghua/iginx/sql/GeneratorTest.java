package cn.edu.tsinghua.iginx.sql;

import cn.edu.tsinghua.iginx.sql.statement.SelectStatement;
import cn.edu.tsinghua.iginx.sql.stub.QueryGeneratorStub;
import org.junit.Test;

public class GeneratorTest {

    @Test
    public void test() {
        String sql = "Select a, b from test where time < 1231312313 AND a > 10";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(sql);
        TestUtils.print(QueryGeneratorStub.getInstance().generate(statement));
    }
}
