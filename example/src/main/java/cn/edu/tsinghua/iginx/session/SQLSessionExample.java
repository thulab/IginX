package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Arrays;
import java.util.List;

public class SQLSessionExample {

    private static Session session;

    private static final String prefix = "us.d1";

    private static final String S1 = "s1";
    private static final String S2 = "s2";
    private static final String S3 = "s3";
    private static final String S4 = "s4";

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 15000L;

    private static final List<String> funcTypeList = Arrays.asList("MAX", "MIN", "FIRST_VALUE", "LAST_VALUE", "SUM", "AVG", "COUNT");

    private static final String insertStrPrefix = "INSERT INTO us.d1 (timestamp, s1, s2, s3, s4) values ";

    private static final String delete = "DELETE FROM us.d1.s1 WHERE time > 105 and time < 115;";

    private static final String simpleQuery = "SELECT s1 FROM us.d1 WHERE time > 100 and time < 120;";
    private static final String valueFilterQuery = "SELECT s1 FROM us.d1 WHERE time > 0 and time < 10000 and s1 > 200 and s1 < 210;";
    private static final String limitQuery = "SELECT s1 FROM us.d1 WHERE time > 0 and time < 10000 limit 10;";
    private static final String limitOffsetQuery = "SELECT s1 FROM us.d1 WHERE time > 0 and time < 10000 limit 10 offset 5;";
    private static final String aggregateQuery = "SELECT %s(%s), %s(%s) FROM us.d1 WHERE time > %s and time < %s;";
    private static final String downSample = "SELECT %s(%s), %s(%s) FROM us.d1 GROUP (%s, %s) BY %s;";
    private static final String lastQuery = "SELECT %s(%s), %s(%s) FROM us.d1 WHERE time > %s;";
    private static final String countAll = "SELECT COUNT(*) FROM us.d1;";

    private static final String deleteTimeSeries = "DELETE TIME SERIES us.d1.s2, us.d1.s4;";
    private static final String addStorageEngines = "ADD STORAGEENGINE (127.0.0.1, 6667, \"iotdb11\", \"username: root, password: root\"), (127.0.0.1, 6668, \"influxdb\", \"key: val\");";

    private static final String countPoints = "COUNT POINTS;";
    private static final String showReplication = "SHOW REPLICA NUMBER;";
    private static final String showTimeSeries = "SHOW TIME SERIES;";
    private static final String showClusterInfo = "SHOW CLUSTER INFO;";
    private static final String clearData = "CLEAR DATA;";

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
