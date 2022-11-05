package cn.edu.tsinghua.iginx.integration.expansion.Parquet;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import org.junit.Test;

public class ParquetHistoryDataGeneratorTest extends BaseHistoryDataGenerator {

    private static final String DATA_DIR = "../parquetData2";

    private static final String FILENAME = "data.parquet";

    @Test
    public void writeHistoryData() throws SQLException, IOException {
        Connection conn = getConnection();
        if (conn == null) {
            fail();
        }

        Path dirPath = Paths.get(DATA_DIR);
        if (Files.notExists(dirPath)) {
            Files.createDirectories(dirPath);
        }

        Statement stmt = conn.createStatement();
        if (stmt == null) {
            fail();
        }

        String tableName = "tmp";
        stmt.execute(String.format("CREATE TABLE %s (time BIGINT, test$sg$cpu_usage DOUBLE, test$sg$engine INTEGER, test$sg$desc VARCHAR);", tableName));
        stmt.execute(String.format(
            "INSERT INTO %s VALUES "
                + "(1, 12.3, 1, 'normal'), "
                + "(2, 23.1, 2, 'normal'), "
                + "(3, 65.2, 1, 'high');", tableName));

        Path parquetPath = Paths.get(DATA_DIR, FILENAME);
        stmt.execute(String.format("COPY (SELECT * FROM %s) TO '%s' (FORMAT 'parquet');", tableName, parquetPath.toString()));
    }

    private static Connection getConnection() {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            return DriverManager.getConnection("jdbc:duckdb:");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
