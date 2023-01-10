package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import org.apache.commons.lang3.RandomStringUtils;

public class ParquetSessionExample {

    private static Session session;

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        // 查看数据分区情况
        long startTimestamp = 0L;
        long step = 10000L;
        for (int i = 0; i < 100; i++) {
            System.out.println("start insert batch data: " + i);
            insertData(startTimestamp, startTimestamp + step);
            startTimestamp += step;
        }

        // 关闭 Session
        session.closeSession();
    }

    private static void insertData(long startTimestamp, long endTimestamp) throws ExecutionException, SessionException {
        String insertStrPrefix = "INSERT INTO us.d1 (key, s1, s2, s3, s4) values ";

        StringBuilder builder = new StringBuilder(insertStrPrefix);

        int size = (int) (endTimestamp - startTimestamp);
        for (int i = 0; i < size; i++) {
            builder.append(", ");
            builder.append("(");
            builder.append(startTimestamp + i).append(", ");
            builder.append(i).append(", ");
            builder.append(i + 1).append(", ");
            builder.append("\"").append(new String(RandomStringUtils.randomAlphanumeric(10).getBytes())).append("\", ");
            builder.append((i + 0.1));
            builder.append(")");
        }
        builder.append(";");

        String insertStatement = builder.toString();

        SessionExecuteSqlResult res = session.executeSql(insertStatement);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            System.out.printf("Insert date execute fail. Caused by: %s.\n", res.getParseErrorMsg());
        }
    }
}
