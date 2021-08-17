package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Arrays;
import java.util.List;

public class SQLSessionExample {

    private static Session session;

    private static final String S1 = "us.d1.s1";
    private static final String S2 = "us.d1.s2";
    private static final String S3 = "us.d1.s3";
    private static final String S4 = "us.d1.s4";

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 15000L;

    private static List<String> funcTypeList = Arrays.asList("MAX", "MIN", "FIRST", "LAST", "SUM", "AVG", "COUNT");

    private static String showReplicationStr = "SHOW REPLICA NUMBER;";
    private static String addStorageEnginesStr = "ADD STORAGEENGINE (127.0.0.1, 6667, IotDB, \"{clause: hello world!  }\"), (127.0.0.1, 6668, InfluxDB, \"{key: val}\");";

    private static String insertStrPrefix = "INSERT INTO us.d1 (timestamp, s1, s2, s3, s4) values ";
    private static String deleteStr = "DELETE FROM us.d1.s1 WHERE time in (105, 115);";

    private static String simpleQueryStr = "SELECT us.d1.s1 FROM us.d1.s1 WHERE time in (100, 120);";
    private static String valueFilterQueryStr = "SELECT us.d1.s1 FROM us.d1.s1 WHERE time in (0, 10000) and us.d1.s1 > 200 and us.d1.s1 < 210;";
    private static String aggregateQueryStr = "SELECT %s(%s), %s(%s) FROM %s, %s WHERE time in (%s, %s);";
    private static String downSampleStr = "SELECT %s(%s), %s(%s) FROM %s, %s WHERE time in (%s, %s) GROUP BY %s;";

    private static String countAll = "SELECT COUNT(*) FROM us.d1;";
    private static String countPoints = "COUNT POINTS";

    private static String clearData = "CLEAR DATA";

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();
        // 插入数据
        execute(buildInsertStr(insertStrPrefix), false);
        execute(countAll, true);
        countPoints(countPoints);
        // 查询副本数
        showReplicationNum(showReplicationStr);
        // 查询数据
        execute(simpleQueryStr, true);
        // 值过滤查询
        execute(valueFilterQueryStr, true);
        // 聚合查询数据
        aggregateQuery();
        // 降采样聚合查询
        downSampleQuery();
        // 删除部分数据
        execute(deleteStr, false);
        // 再次查询数据
        execute(simpleQueryStr, true);
        // 清空数据
        execute(clearData, false);
        countPoints(countPoints);
        // 增加存储引擎，测试该项前保证本地启动了对应的数据库实例
//        execute(addStorageEnginesStr);
        // 关闭 Session
        session.openSession();
    }

    public static void showReplicationNum(String statement) throws SessionException, ExecutionException {
        SessionExecuteSqlResult res = session.executeSql(statement);
        System.out.println("Replication num: " + res.getReplicaNum());
        System.out.println();
    }

    public static void countPoints(String statement) throws ExecutionException, SessionException {
        SessionExecuteSqlResult res = session.executeSql(statement);
        System.out.println("Points num: " + res.getReplicaNum());
        System.out.println();
    }

    public static void aggregateQuery() throws SessionException, ExecutionException {
        for (String type : funcTypeList) {
            execute(String.format(aggregateQueryStr, type, S1, type, S2, S1, S2, "0", "1000"), true);
        }
    }

    public static void downSampleQuery() throws SessionException, ExecutionException {
        for (String type : funcTypeList) {
            execute(String.format(downSampleStr, type, S1, type, S4, S1, S4, "0", "1000", "100ms"), true);
        }
    }

    public static void execute(String statement, boolean needPrint) throws SessionException, ExecutionException {
        SessionExecuteSqlResult res = session.executeSql(statement);
        System.out.println("SQL Type: " + res.getSqlType());
        if (needPrint) {
            res.print();
        }
        System.out.println();
    }

    private static String buildInsertStr(String insertStrPrefix) {
        StringBuilder builder = new StringBuilder(insertStrPrefix);

        int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
        for (int i = 0; i < size; i++) {
            builder.append(", ");
            builder.append("(");
            builder.append((START_TIMESTAMP + i) + ", ");
            builder.append(i + ", ");
            builder.append((i + 1) + ", ");
            builder.append("\"" + new String(RandomStringUtils.randomAlphanumeric(10).getBytes()) + "\", ");
            builder.append((i + 0.1));
            builder.append(")");
        }
        builder.append(";");

        return builder.toString();
    }
}
