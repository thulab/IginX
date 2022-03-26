import cn.edu.tsinghua.iginx.jdbc.IginXPreparedStatement;
import org.apache.commons.lang3.RandomStringUtils;

import java.sql.*;

public class ExampleTest {

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 100L;
    private static Connection connection;
    private static Statement statement;
    private static PreparedStatement preparedStatement;
    private static String prefix = "us.d2";

    private static String S1 = "long";
    private static String S2 = "double";
    private static String S3 = "boolean";
    private static String S4 = "string";

    public static void main(String[] args) throws SQLException {
        // Create connection
        connection = getConnection();
        if (connection == null) {
            System.out.println("create connection fail.");
            return;
        }
        // Create statement
        statement = connection.createStatement();
        if (statement == null) {
            System.out.println("create statement fail.");
            return;
        }

        // Insert use batch
        // batch only support update statement.
        int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
        String insertClause = "INSERT INTO us.d2 (timestamp, long, double, boolean, string) values (%s, %s, %s, %s, %s);";
        for (int i = 0; i < size; i++) {
            String sql = String.format(insertClause,
                START_TIMESTAMP + i, // timestamp
                i + 1, // long
                i + 0.5, // double
                i % 2 == 0, //boolean
                "\"" + new String(RandomStringUtils.randomAlphanumeric(10).getBytes()) + "\"" // string
            );
            statement.addBatch(sql);
        }
        statement.executeBatch();
        statement.clearBatch();

        // Insert use execute
        String sql = String.format(insertClause,
            100, // timestamp
            101, // long
            100.5, // double
            true, //boolean
            "\"" + new String(RandomStringUtils.randomAlphanumeric(10).getBytes()) + "\"" // string
        );
        System.out.println("sql: " + sql);
        statement.execute(sql);

        // Insert use executeUpdate
        sql = String.format(insertClause,
            101, // timestamp
            102, // long
            101.5, // double
            false, //boolean
            "\"" + new String(RandomStringUtils.randomAlphanumeric(10).getBytes()) + "\"" // string
        );
        System.out.println("sql: " + sql);
        statement.executeUpdate(sql);

        // Full query use executeQuery
        String fullQueryClause = "SELECT %s, %s, %s, %s FROM %s WHERE TIME > %s AND TIME < %s;";
        sql = String.format(fullQueryClause,
            S1, S2, S3, S4, // select
            prefix, // from
            0, 200 // time range
        );
        ResultSet resultSet = statement.executeQuery(sql);
        System.out.println("sql: " + sql);
        outputResult(resultSet);

        // Time range query use execute
        String timeRangeClause = "SELECT %s FROM %s WHERE time > 50 AND TIME < 100;";
        sql = String.format(timeRangeClause, S1, prefix);
        if (statement.execute(sql)) {
            resultSet = statement.getResultSet();
        }
        System.out.println("sql: " + sql);
        outputResult(resultSet);

        // DownSample query use executeQuery
        String downSampleClause = "SELECT %s(%s) FROM %s GROUP (%s, %s) BY %s;";
        sql = String.format(downSampleClause, "MAX", S2, prefix, "0", "200", "10ms");
        resultSet = statement.executeQuery(sql);
        System.out.println("sql: " + sql);
        outputResult(resultSet);

        // Aggregate query use execute
        String aggregateClause = "SELECT COUNT(%s) FROM %s;";
        sql = String.format(aggregateClause, S4, prefix);
        if (statement.execute(sql)) {
            resultSet = statement.getResultSet();
        }
        System.out.println("sql: " + sql);
        outputResult(resultSet);

        // Close statement
        statement.close();

        // Create prepareStatement
        String preparedClause = "SELECT %s FROM %s WHERE TIME > ? AND TIME < ? AND %s < ?;";
        preparedStatement = connection.prepareStatement(String.format(preparedClause, S1, prefix, S1));
        if (statement == null) {
            System.out.println("create statement fail.");
            return;
        }
        // set params and query
        preparedStatement.setLong(1, 15); // start time
        preparedStatement.setLong(2, 25); // end time
        preparedStatement.setLong(3, 20); // S3 < 20
        resultSet = preparedStatement.executeQuery();
        System.out.println("sql: " + ((IginXPreparedStatement) preparedStatement).getCompleteSql());
        outputResult(resultSet);

        // Clear and query.
        preparedStatement.clearParameters();
        preparedStatement.setLong(1, 60); // start time
        preparedStatement.setLong(2, 99); // end time
        preparedStatement.setLong(3, 86); // S3 < 86
        resultSet = preparedStatement.executeQuery();
        System.out.println("sql: " + ((IginXPreparedStatement) preparedStatement).getCompleteSql());
        outputResult(resultSet);

        // Close prepareStatement
        preparedStatement.close();

        // clear data
        statement = connection.createStatement();
        sql = "clear data;";
        System.out.println("sql: " + sql);
        statement.execute(sql);
        statement.close();

        // CLose connection
        connection.close();
    }

    private static Connection getConnection() {
        String driver = "cn.edu.tsinghua.iginx.jdbc.IginXDriver";
        String url = "jdbc:iginx://127.0.0.1:6888/";

        String username = "root";
        String password = "root";

        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private static void outputResult(ResultSet resultSet) throws SQLException {
        if (resultSet != null) {
            System.out.println("--------------------------");
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                System.out.print(metaData.getColumnLabel(i + 1) + " ");
            }
            System.out.println();
            while (resultSet.next()) {
                for (int i = 1; ; i++) {
                    System.out.print(resultSet.getString(i));
                    if (i < columnCount) {
                        System.out.print(", ");
                    } else {
                        System.out.println();
                        break;
                    }
                }
            }
            System.out.println("--------------------------\n");
        }
    }
}
