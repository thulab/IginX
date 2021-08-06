import cn.edu.tsinghua.iginx.jdbc.IginXPreparedStatement;
import org.junit.Assert;
import org.junit.Test;

public class PreparedStatementTest {

    @Test
    public void testSetParams() {
        String preSQL = "SELECT a, b, c, d FROM root.sg WHERE TIME in(?, ?) and a > ? or b < ? and c = ? and d = ?;";
        IginXPreparedStatement ps = new IginXPreparedStatement(null, null, preSQL);
        ps.setLong(1, 10);
        ps.setLong(2, 15);
        ps.setFloat(3, 66.8f);
        ps.setDouble(4, 99.9);
        ps.setString(5, "abc");
        ps.setBoolean(6, true);

        String expectedSQL = "SELECT a, b, c, d FROM root.sg WHERE TIME in(10, 15) and a > 66.8 or b < 99.9 and c = abc and d = true;";
        String completeSQL = ps.getCompleteSql();
        Assert.assertEquals(expectedSQL, completeSQL);
    }

    @Test
    public void testSetParamsWithSkipDoubleQuotes() {
        String preSQL = "SELECT a, b FROM root.sg WHERE TIME in(10, 25) and a > ? and b = \"asda?asd\";";
        IginXPreparedStatement ps = new IginXPreparedStatement(null, null, preSQL);
        ps.setLong(1, 10);

        String expectedSQL = "SELECT a, b FROM root.sg WHERE TIME in(10, 25) and a > 10 and b = \"asda?asd\";";
        String completeSQL = ps.getCompleteSql();
        Assert.assertEquals(expectedSQL, completeSQL);
    }

    @Test
    public void testSetParamsWithSkipSingleQuote() {
        String preSQL = "SELECT a, b FROM root.sg WHERE TIME in(10, 25) and a > ? and b = \'asda?asd\';";
        IginXPreparedStatement ps = new IginXPreparedStatement(null, null, preSQL);
        ps.setLong(1, 10);

        String expectedSQL = "SELECT a, b FROM root.sg WHERE TIME in(10, 25) and a > 10 and b = \'asda?asd\';";
        String completeSQL = ps.getCompleteSql();
        Assert.assertEquals(expectedSQL, completeSQL);
    }
}
