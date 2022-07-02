package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UDFExample {

    private static Session session;

    private static final String S1 = "udf.value1";
    private static final String S2 = "udf.value2";
    private static final String S3 = "udf.value3";
    private static final String S4 = "udf.value4";

    private static final String REGISTER_SQL_FORMATTER = "REGISTER %s PYTHON TASK %s IN %s AS %s";
    private static final String DROP_SQL_FORMATTER = "DROP PYTHON TASK %s";
    private static final String SHOW_REGISTER_TASK_SQL = "SHOW REGISTER PYTHON TASK;";

    /*
    private static final String FILE_DIR = String.join(File.separator, System.getProperty("user.dir"),
        "example", "src", "main", "resources");*/
    private static final String FILE_DIR = "/root/IginX/example/src/main/resources";

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 1000L;

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("120.48.93.68", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        // 准备数据
        session.deleteColumns(Collections.singletonList("*"));
        prepareData();

        // 查询序列
        SessionExecuteSqlResult result = session.executeSql("show time series");
        result.print(false, "ms");

        // 注册UDTF
        String registerSQL = String.format(REGISTER_SQL_FORMATTER, "UDTF", "\"UDFSin\"", "\"" + FILE_DIR + '/' + "udtf_sin.py" + "\"", "\"sin\"");
        session.executeSql(registerSQL);
        registerSQL = String.format(REGISTER_SQL_FORMATTER, "UDAF", "\"UDFCount\"", "\"" + FILE_DIR + '/' + "udaf_count.py" + "\"", "\"py_count\"");
        session.executeSql(registerSQL);

        // 查询已注册的UDF
        result = session.executeSql(SHOW_REGISTER_TASK_SQL);
        result.print(false, "ms");

        // 使用已注册的UDF
        result = session.executeSql("SELECT SIN(*) FROM udf WHERE value1 < 10;");
        result.print(true, "ms");

        result = session.executeSql("SELECT PY_COUNT(*) FROM udf WHERE value1 < 10;");
        result.print(true, "ms");

        // 注销UDF
        String dropSQL = String.format(DROP_SQL_FORMATTER, "\"sin\"");
        session.executeSql(dropSQL);
        dropSQL = String.format(DROP_SQL_FORMATTER, "\"py_count\"");
        session.executeSql(dropSQL);

        // 查询已注册的UDF
        result = session.executeSql(SHOW_REGISTER_TASK_SQL);
        result.print(false, "ms");
    }

    private static void prepareData() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = START_TIMESTAMP + i;
            Object[] values = new Object[4];
            for (long j = 0; j < 4; j++) {
                values[(int) j] = i + j;
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            dataTypeList.add(DataType.LONG);
        }

        System.out.println("insertRowRecords...");
        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }
}
