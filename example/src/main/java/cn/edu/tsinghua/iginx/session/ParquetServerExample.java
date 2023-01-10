package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import java.util.Collections;

public class ParquetServerExample {

    private static Session session1;

    private static Session session2;

    /**
     * session1 parquet use local
     * session2 parquet use remote
     * */
    public static void main(String[] args) throws SessionException, ExecutionException {
        // init
        session1 = new Session("127.0.0.1", 6888, "root", "root");
        session1.openSession();
        session2 = new Session("127.0.0.1", 6889, "root", "root");
        session2.openSession();

        // local insert
        session1.executeSql(
            "INSERT INTO test(key, s1, s2, s3) VALUES "
                + "(1, 1, 1.5, \"test1\"), "
                + "(2, 2, 2.5, \"test2\"), "
                + "(3, 3, 3.5, \"test3\"), "
                + "(4, 4, 4.5, \"test4\"), "
                + "(5, 5, 5.5, \"test5\"); "
        );

        System.out.println("================================");

        // local query and get time series
        System.out.println("local result:");
        SessionExecuteSqlResult result1 = session1.executeSql("SELECT * FROM test");
        result1.print(false, "");

        result1 = session1.executeSql("SHOW TIME SERIES");
        result1.print(false, "");

        System.out.println("================================");

        // remote query and get time series
        System.out.println("remote result:");
        SessionExecuteSqlResult result2 = session2.executeSql("SELECT * FROM test");
        result2.print(false, "");

        result2 = session2.executeSql("SHOW TIME SERIES");
        result2.print(false, "");

        System.out.println("================================");

        // remote delete data and local query
        session2.executeSql("DELETE FROM test.s3 WHERE key > 3");

        result1 = session1.executeSql("SELECT * FROM test");
        result1.print(false, "");

        System.out.println("================================");

        // remote delete cols and local query
        session2.deleteColumns(Collections.singletonList("test.s3"));

        result1 = session1.executeSql("SELECT * FROM test");
        result1.print(false, "");

        result1 = session1.executeSql("SHOW TIME SERIES");
        result1.print(false, "");

        System.out.println("================================");

        session2.executeSql("INSERT INTO test(key, s4, s5) VALUES "
            + "(6, 6.1, \"test6\"), "
            + "(7, 7.1, \"test7\"), "
            + "(8, 8.1, \"test8\")"
        );

        result1 = session1.executeSql("SELECT * FROM test");
        result1.print(false, "");

        result1 = session1.executeSql("SHOW TIME SERIES");
        result1.print(false, "");

        session1.closeSession();
        session2.closeSession();
    }
}
