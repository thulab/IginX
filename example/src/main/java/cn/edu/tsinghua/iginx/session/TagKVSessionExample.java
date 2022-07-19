package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.*;

public class TagKVSessionExample {

    private static final String S1 = "ln.wf02.s";
    private static final String S2 = "ln.wf02.v";
    private static final String S3 = "ln.wf03.s";
    private static final String S4 = "ln.wf03.v";

    private static final long COLUMN_START_TIMESTAMP = 1L;
    private static final long COLUMN_END_TIMESTAMP = 10L;

    private static Session session;

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        // 列式插入对齐数据
        insertColumnRecords();

        // 查询数据
        queryData();

        // 关闭 Session
        session.closeSession();
    }

    private static void insertColumnRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);


        int size = (int) (COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i + COLUMN_START_TIMESTAMP;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                if (i < 2) {
                    values[(int) j] = i + j;
                } else {
                    values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        List<Map<String, String>> tagsList = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            Map<String, String> tags = new HashMap<>();
            tags.put("k", "v"+i);
            tagsList.add(tags);
        }

        System.out.println("insertColumnRecords...");
        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
    }

    private static void queryData() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        long startTime = COLUMN_START_TIMESTAMP + 2;
        long endTime = COLUMN_END_TIMESTAMP - 2;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime, null);
        dataSet.print();

        Map<String, List<String>> tagsList = new HashMap<>();
        tagsList.put("k", Arrays.asList("v1", "v3"));

        dataSet = session.queryData(paths, startTime, endTime, tagsList);
        dataSet.print();
    }
}
