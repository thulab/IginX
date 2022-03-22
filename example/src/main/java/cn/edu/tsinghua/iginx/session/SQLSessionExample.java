package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Arrays;
import java.util.List;

public class SQLSessionExample {

    private static Session session;

    private final static String prefix = "us.d1";

    private final static String S1 = "s1";
    private final static String S2 = "s2";
    private final static String S3 = "s3";
    private final static String S4 = "s4";

    private final static long START_TIMESTAMP = 0L;
    private final static long END_TIMESTAMP = 15000L;

    private final static List<String> funcTypeList = Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");

    private final static String insertStrPrefix = "INSERT INTO us.d1 (timestamp, s1, s2, s3, s4) values ";

    private final static String delete = "DELETE FROM us.d1.s1 WHERE time in (105, 115);";

    private final static String simpleQuery = "SELECT s1 FROM us.d1 WHERE time in (100, 120);";
    private final static String valueFilterQuery = "SELECT s1 FROM us.d1 WHERE time in (0, 10000) and s1 > 200 and s1 < 210;";
    private final static String limitQuery = "SELECT s1 FROM us.d1 WHERE time in (0, 10000) limit 10;";
    private final static String limitOffsetQuery = "SELECT s1 FROM us.d1 WHERE time in (0, 10000) limit 10 offset 5;";
    private final static String aggregateQuery = "SELECT %s(%s), %s(%s) FROM us.d1 WHERE time in (%s, %s);";
    private final static String downSample = "SELECT %s(%s), %s(%s) FROM us.d1 WHERE time in (%s, %s) GROUP BY %s;";
    private final static String lastQuery = "SELECT %s(%s), %s(%s) FROM us.d1 WHERE time in (%s, INF);";
    private final static String countAll = "SELECT COUNT(*) FROM us.d1;";

    private final static String deleteTimeSeries = "DELETE TIMESERIES us.d1.s2, us.d1.s4;";
    private final static String addStorageEngines = "ADD STORAGEENGINE (127.0.0.1, 6667, \"iotdb11\", \"username: root, password: root\"), (127.0.0.1, 6668, \"influxdb\", \"key: val\");";

    private final static String countPoints = "COUNT POINTS;";
    private final static String showReplication = "SHOW REPLICA NUMBER;";
    private final static String showTimeSeries = "SHOW TIMESERIES;";
    private final static String showSubTimeSeries = "SHOW SUB TIMESERIES us;";
    private final static String showClusterInfo = "SHOW CLUSTER INFO;";
    private final static String clearData = "CLEAR DATA;";

    private final static String createUser = "CREATE USER root1 IDENTIFIED BY root1;";
    private final static String grantUser = "GRANT WRITE, READ TO USER root1;";
    private final static String changePassword = "SET PASSWORD FOR root1 = PASSWORD(root2);";
    private final static String showUser = "SHOW USER;";
    private final static String dropUser = "DROP USER root1;";

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();
        // 插入数据
        execute(buildInsertStr(insertStrPrefix), false);
        // 查询每条路径的数据量
        execute(countAll, true);
        // 查询点数
        execute(countPoints, true);
        // 查询时间序列
        execute(showTimeSeries, true);
        // 查询子时间序列
        execute(showSubTimeSeries, true);
        // 查询副本数
        execute(showReplication, true);
        // 查询集群信息
        execute(showClusterInfo, true);
        // 查询数据
        execute(simpleQuery, true);
        // limit/offset查询
        execute(limitQuery, true);
        execute(limitOffsetQuery, true);
        // 值过滤查询
        execute(valueFilterQuery, true);
        // 最新值查询
        lastQuery();
        // 聚合查询数据
        aggregateQuery();
        // 降采样聚合查询
        downSampleQuery();
        // 删除部分数据
        execute(delete, false);
        // 再次查询数据
        execute(simpleQuery, true);
        // 删除序列
        execute(deleteTimeSeries, false);
        // 查询点数
        execute(countPoints, true);
        // 查询时间序列
        execute(showTimeSeries, true);
        // 清空数据
        execute(clearData, false);
        // 查询点数
        execute(countPoints, true);
        // 查询时间序列
        execute(showTimeSeries, true);
        // 增加存储引擎，测试该项前保证本地启动了对应的数据库实例
//        execute(addStorageEnginesStr);
        // 新增用户
        execute(createUser, false);
        execute(showUser, true);
        // 更新用户
        execute(grantUser, false);
        execute(showUser, true);
        // 更改密码
        execute(changePassword, false);
        // 删除用户
        execute(dropUser, false);
        execute(showUser, true);
        // 关闭 Session
        session.openSession();
    }

    public static void lastQuery() throws SessionException, ExecutionException {
        execute(String.format(lastQuery, "LAST", S2, "LAST", S3, 0), true);
    }

    public static void aggregateQuery() throws SessionException, ExecutionException {
        for (String type : funcTypeList) {
            execute(String.format(aggregateQuery, type, S1, type, S2, 0, 1000), true);
        }
    }

    public static void downSampleQuery() throws SessionException, ExecutionException {
        for (String type : funcTypeList) {
            execute(String.format(downSample, type, S1, type, S4, 0, 1000, "100ms"), true);
        }
    }

    public static void execute(String statement, boolean needPrint) throws SessionException, ExecutionException {
        SessionExecuteSqlResult res = session.executeSql(statement);
        if (!statement.startsWith("INSERT"))
            System.out.println("Statement:" + statement);
        System.out.println("SQL Type: " + res.getSqlType());
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            System.out.println(res.getParseErrorMsg());
        } else if (needPrint) {
            res.print(false, "");
        }
        System.out.println();
    }

    private static String buildInsertStr(String insertStrPrefix) {
        StringBuilder builder = new StringBuilder(insertStrPrefix);

        int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
        for (int i = 0; i < size; i++) {
            builder.append(", ");
            builder.append("(");
            builder.append(START_TIMESTAMP + i).append(", ");
            builder.append(i).append(", ");
            builder.append(i + 1).append(", ");
            builder.append("\"").append(new String(RandomStringUtils.randomAlphanumeric(10).getBytes())).append("\", ");
            builder.append((i + 0.1));
            builder.append(")");
        }
        builder.append(";");

        return builder.toString();
    }
}
