package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.List;

public class OpenTSDBSessionExample {

    private static final String S1 = "sg.d1.s1";
    private static final String S2 = "sg.d1.s2";
    private static final String S3 = "sg.d2.s1";
    private static final String S4 = "sg.d3.s1";
    private static final long COLUMN_START_TIMESTAMP = 1L;
    private static final long COLUMN_END_TIMESTAMP = 10L;
    private static final long NON_ALIGNED_COLUMN_START_TIMESTAMP = 11L;
    private static final long NON_ALIGNED_COLUMN_END_TIMESTAMP = 20L;
    private static final long ROW_START_TIMESTAMP = 21L;
    private static final long ROW_END_TIMESTAMP = 30L;
    private static final long NON_ALIGNED_ROW_START_TIMESTAMP = 31L;
    private static final long NON_ALIGNED_ROW_END_TIMESTAMP = 40L;
    private static final int INTERVAL = 10;
    private static Session session;

    public static void main(String[] args) throws SessionException, ExecutionException, InterruptedException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        insertRowRecords();

//        queryData();

//        deleteDataInColumns();

        session.closeSession();
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        int size = (int) (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = ROW_START_TIMESTAMP + i;
            Object[] values = new Object[2];
            for (long j = 0; j < 2; j++) {
                values[(int) j] = i + j;
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }

        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void queryData() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        dataSet.print();
    }

    private static void deleteDataInColumns() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 50L;
        long endTime = ROW_START_TIMESTAMP + 50L;

        session.deleteDataInColumns(paths, startTime, endTime);
    }
}
