package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.rest.MetricsResource;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CombinedInsertTests {

    private static final String S1 = "sg.d1.s1";
    private static final String S2 = "sg.d1.s2";
    private static final String S3 = "sg.d2.s1";
    private static final String S4 = "sg.d3.s1";
    private static final long COLUMN_START_TIMESTAMP = 1L;
    private static final long COLUMN_END_TIMESTAMP = 10000L;
    private static final long NON_ALIGNED_COLUMN_START_TIMESTAMP = 10001L;
    private static final long NON_ALIGNED_COLUMN_END_TIMESTAMP = 20000L;
    private static final long ROW_START_TIMESTAMP = 20001L;
    private static final long ROW_END_TIMESTAMP = 30000L;
    private static final long NON_ALIGNED_ROW_START_TIMESTAMP = 30001L;
    private static final long NON_ALIGNED_ROW_END_TIMESTAMP = 40000L;

    private static Session session = null;

    private static SessionPool sessionPool = null;

    public CombinedInsertTests(Session passedSession){
        session = passedSession;
    }

    public CombinedInsertTests(SessionPool passedSessionPool){
        sessionPool = passedSessionPool;
    }

    public void testInserts() throws SessionException, ExecutionException {
        // 列式插入对齐数据
        insertColumnRecords();
        // 列式插入非对齐数据
        insertNonAlignedColumnRecords();
        // 行式插入对齐数据
        insertRowRecords();
        // 行式插入非对齐数据
        insertNonAlignedRowRecords();
    }

    private static void insertColumnRecords() throws SessionException, ExecutionException {
        String PREFIX = "insertCR.";
        List<String> paths = new ArrayList<>();
        paths.add(PREFIX+S1);
        paths.add(PREFIX+S2);
        paths.add(PREFIX+S3);
        paths.add(PREFIX+S4);

        int size = (int) (COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i + COLUMN_START_TIMESTAMP;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                if (i%2==1) {
                    values[(int) j] = i + j;
                } else {
                    values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (long j = 0; j < 4; j++) {
            if (j%2==1)
                dataTypeList.add(DataType.LONG);
            else
                dataTypeList.add(DataType.BINARY);
        }

        System.out.println("insertColumnRecords...");
        if(session != null)
            session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
        else if(sessionPool != null)
            sessionPool.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertNonAlignedColumnRecords() throws SessionException, ExecutionException {
        String PREFIX = "insertCNAR.";
        List<String> paths = new ArrayList<>();
        paths.add(PREFIX+S1);
        paths.add(PREFIX+S2);
        paths.add(PREFIX+S3);
        paths.add(PREFIX+S4);

        int size = (int) (NON_ALIGNED_COLUMN_END_TIMESTAMP - NON_ALIGNED_COLUMN_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i + NON_ALIGNED_COLUMN_START_TIMESTAMP;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                if (j >= size - 50) {
                    values[(int) j] = null;
                } else {
                    if (i%2==0) {
                        values[(int) j] = i + j;
                    } else {
                        values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                    }
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (long j = 0; j < 4; j++) {
            if (j%2==0)
                dataTypeList.add(DataType.LONG);
            else
                dataTypeList.add(DataType.BINARY);
        }

        System.out.println("insertNonAlignedColumnRecords...");
        if(session != null)
            session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
        else if(sessionPool != null)
            sessionPool.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        String PREFIX = "insertRR.";
        List<String> paths = new ArrayList<>();
        paths.add(PREFIX+S1);
        paths.add(PREFIX+S2);
        paths.add(PREFIX+S3);
        paths.add(PREFIX+S4);
        paths.add(PREFIX+S3+S4);

        int size = (int) (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP + 1);
        int colNum = 5;
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = ROW_START_TIMESTAMP + i;
            Object[] values = new Object[colNum];
            for (long j = 0; j < colNum; j++) {
                if (j%2==0) {
                    values[(int) j] = i + j;
                } else {
                    values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (long j = 0; j < colNum; j++) {
            if (j%2==0)
                dataTypeList.add(DataType.LONG);
            else
                dataTypeList.add(DataType.BINARY);
        }

        System.out.println("insertRowRecords...");
        if(session != null)
            session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
        else if(sessionPool != null)
            sessionPool.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertNonAlignedRowRecords() throws SessionException, ExecutionException {
        String PREFIX = "insertNARR.";
        List<String> paths = new ArrayList<>();
        paths.add(PREFIX+S1);
        paths.add(PREFIX+S2);
        paths.add(PREFIX+S3);
        paths.add(PREFIX+S4);

        int size = (int) (NON_ALIGNED_ROW_END_TIMESTAMP - NON_ALIGNED_ROW_START_TIMESTAMP + 1);
        int colNum = 4;
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = NON_ALIGNED_ROW_START_TIMESTAMP + i;
            Object[] values = new Object[colNum];
            for (long j = 0; j < colNum; j++) {
                if ((i + j) % 2 == 0) {
                    values[(int) j] = null;
                } else {
                    if (j%2==0){
                        values[(int) j] = i + j;
                    } else {
                        values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                    }
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (long j = 0; j < colNum; j++) {
            if (j%2==0)
                dataTypeList.add(DataType.LONG);
            else
                dataTypeList.add(DataType.BINARY);
        }

        System.out.println("insertNonAlignedRowRecords...");
        if(session != null)
            session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, null);
        else if(sessionPool != null)
            sessionPool.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

}
