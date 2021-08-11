import cn.edu.tsinghua.iginx.jdbc.IginXStatement;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BatchTest {

    @Test
    public void testBatch() throws SQLException {
        List<String> sqlList = new ArrayList<>(Arrays.asList(
                "INSERT INTO test.batch (a, b, c) values (1, 1.1, \"one\");",
                "INSERT INTO test.batch (a, b, c) values (2, 2.1, \"two\");",
                "INSERT INTO test.batch (a, b, c) values (3, 3.1, \"three\");",
                "DELETE FROM test.batch.c WHERE c = \"two\"",
                "DELETE FROM test.batch.c WHERE c = \"three\"",
                "ADD STORAGEENGINE (127.0.0.1, 6667, IOTDB, \"{\"hello\": \"world\"}\");"
        ));

        IginXStatement statement = new IginXStatement(null, null);

        for (String sql: sqlList) {
            statement.addBatch(sql);
        }
        Assert.assertEquals(sqlList, statement.getBatchSQLList());

        statement.clearBatch();
        Assert.assertEquals(Collections.emptyList(), statement.getBatchSQLList());
    }

    /* Batch query is not supported. */

    @Test(expected = SQLException.class)
    public void testBatchWithSelect() throws SQLException {
        IginXStatement statement = new IginXStatement(null, null);
        String sql = "SELECT a FROM test.batch WHERE a < 10;";
        statement.addBatch(sql);
    }

    @Test(expected = SQLException.class)
    public void testBatchWithShow() throws SQLException {
        IginXStatement statement = new IginXStatement(null, null);
        String sql = "SHOW REPLICATION;";
        statement.addBatch(sql);
    }
}
