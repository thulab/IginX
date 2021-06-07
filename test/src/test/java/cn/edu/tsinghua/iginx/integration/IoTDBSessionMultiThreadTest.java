package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class IoTDBSessionMultiThreadTest {


    private static final Logger logger = LoggerFactory.getLogger(IoTDBSessionIT.class);
    private static final String DATABASE_NAME = "sg1";
    private static final String COLUMN_D1_S1 = "sg1.d1.s1";
    private static final String COLUMN_D2_S2 = "sg1.d2.s2";
    private static final String COLUMN_D3_S3 = "sg1.d3.s3";
    private static final String COLUMN_D4_S4 = "sg1.d4.s4";
    private static final String COLUMN_D5_S5 = "sg1.d5.s5";
    private static final long TIME_PERIOD = 100000L;
    private static final long START_TIME = 0L;
    private static final long END_TIME = START_TIME + TIME_PERIOD - 1;
    private static Session session;
    private List<String> paths = new ArrayList<>();
    private static final double delta = 1e-7;

    @Before
    public void setUp() {
        try {
            paths.add(COLUMN_D1_S1);
            paths.add(COLUMN_D2_S2);
            paths.add(COLUMN_D3_S3);
            paths.add(COLUMN_D4_S4);
            paths.add(COLUMN_D5_S5);
            session = new Session("127.0.0.1", 6888, "root", "root");
            session.openSession();
            session.createDatabase(DATABASE_NAME);
            addColumns();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws ExecutionException, SessionException {

        // delete metadata from ZooKeeper

        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper("127.0.0.1:2181", 5000, null);
            ZKUtil.deleteRecursive(zk, "/iginx");
            ZKUtil.deleteRecursive(zk, "/storage");
            ZKUtil.deleteRecursive(zk, "/unit");
            ZKUtil.deleteRecursive(zk, "/lock");
            ZKUtil.deleteRecursive(zk, "/fragment");
        } catch (IOException | InterruptedException | KeeperException e) {
            logger.error(e.getMessage());
        }

        // delete data from IoTDB
        org.apache.iotdb.session.Session iotdbSession = null;
        try {
            iotdbSession = new org.apache.iotdb.session.Session("127.0.0.1", 6667, "root", "root");
            iotdbSession.open(false);
            iotdbSession.executeNonQueryStatement("DELETE TIMESERIES root.*");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }
        // close session
        try {
            iotdbSession.close();
            if (zk != null) {
                zk.close();
            }
            session.closeSession();
        } catch (InterruptedException | IoTDBConnectionException e) {
            logger.error(e.getMessage());
        }/*
        session.closeSession();*/
    }

    @Test
    public void sessionForStorageQueryTest() throws SessionException, InterruptedException, ExecutionException {
        Task[] tasks = new Task[5];
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            tasks[i] = new Task(1, getSinglePathList(i), START_TIME, END_TIME, TIME_PERIOD, 1);
            threads[i] = new Thread(tasks[i]);
        }
        for (int i = 0; i < 5; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 5; i++) {
            threads[i].join();
        }
        //TODO 观察数据是否已经被插入完毕,和去掉这个sleep之后是否有区别
        Thread.sleep(10000);
        //query
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(5, resPaths.size());
        assertEquals(TIME_PERIOD, len);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());
        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 5; j++) {
                assertEquals(getPathNum(resPaths.get(j)) + timestamp, result.get(j));
            }
        }

        Thread.sleep(5000);

        // Test max function
        SessionAggregateQueryDataSet maxDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> maxResPaths = maxDataSet.getPaths();
        Object[] maxResult = maxDataSet.getValues();
        assertEquals(paths.size(), maxResPaths.size());
        assertEquals(paths.size(), maxDataSet.getValues().length);
        for (int i = 0; i < 5; i++) {
            assertEquals(getPathNum(maxResPaths.get(i)) + END_TIME, maxResult[i]);
        }

        // Test avg function
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(paths.size(), avgResPaths.size());
        assertEquals(paths.size(), avgDataSet.getValues().length);
        for (int i = 0; i < 5; i++) {
            assertEquals(getPathNum(avgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0, (double)avgResult[i], 0.01);
        }
    }

    @Test
    public void sessionForTimeQueryTest() throws SessionException, InterruptedException, ExecutionException {
        Task[] tasks = new Task[5];
        Thread[] threads = new Thread[5];
        for (int i = 0; i < 5; i++) {
            tasks[i] = new Task(1, paths, START_TIME + i, END_TIME - (4 - i), TIME_PERIOD / 5, 5);
            threads[i] = new Thread(tasks[i]);
        }
        for (int i = 0; i < 5; i++) {
            threads[i].start();
        }
        for (int i = 0; i < 5; i++) {
            threads[i].join();
        }
        //TODO 观察数据是否已经被插入完毕,和去掉这个sleep之后是否有区别
        Thread.sleep(10000);
        //query
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(5, resPaths.size());
        assertEquals(TIME_PERIOD, len);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());
        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 5; j++) {
                assertEquals(getPathNum(resPaths.get(j)) + timestamp, result.get(j));
            }
        }

        // Test max function
        SessionAggregateQueryDataSet maxDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> maxResPaths = maxDataSet.getPaths();
        Object[] maxResult = maxDataSet.getValues();
        assertEquals(paths.size(), maxResPaths.size());
        assertEquals(paths.size(), maxDataSet.getValues().length);
        for (int i = 0; i < 5; i++) {
            assertEquals(getPathNum(maxResPaths.get(i)) + END_TIME, maxResult[i]);
        }

        // Test avg function
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(paths.size(), avgResPaths.size());
        assertEquals(paths.size(), avgDataSet.getValues().length);
        for (int i = 0; i < 5; i++) {
            assertEquals(getPathNum(avgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0, (double)avgResult[i], 0.01);
        }
    }

    @Test
    public void sessionForStoragePartDeleteTest() throws ExecutionException, SessionException, InterruptedException {
        insertRecords();

        //TODO 观察数据是否已经被插入完毕,和去掉这个sleep之后是否有区别
        Thread.sleep(10000);

        SessionQueryDataSet dataSet1 = session.queryData(paths, START_TIME, END_TIME + 1);
        int len1 = dataSet1.getTimestamps().length;
        List<String> resPaths1 = dataSet1.getPaths();
        assertEquals(5, resPaths1.size());
        assertEquals(TIME_PERIOD, len1);
        assertEquals(TIME_PERIOD, dataSet1.getValues().size());

        long delStartTime = START_TIME + TIME_PERIOD / 5;
        long delEndTime = START_TIME + TIME_PERIOD / 10 * 9;
        long delTimePeriod = delEndTime - delStartTime + 1;

        int threadNum = 1;
        Task[] tasks = new Task[threadNum];
        Thread[] threads = new Thread[threadNum];

        for (int i = 0; i < threadNum; i++) {
            tasks[i] = new Task(2, getSinglePathList(i), delStartTime,
                    delEndTime, delTimePeriod, 1);
            threads[i] = new Thread(tasks[i]);
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].join();
        }

        Thread.sleep(5000);

        //query
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(5, resPaths.size());
        assertEquals(TIME_PERIOD, len);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());

        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 5; j++) {
                if (delStartTime <= timestamp && timestamp <= delEndTime) {
                    if (getPathNum(resPaths.get(j)) >= threadNum){
                        assertEquals(timestamp + getPathNum(resPaths.get(j)), result.get(j));
                    } else {
                        assertNull(result.get(j));
                    }
                } else {
                    assertEquals(getPathNum(resPaths.get(j)) + timestamp, result.get(j));
                }
            }
        }

        // Test avg function
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(paths.size(), avgResPaths.size());
        assertEquals(paths.size(), avgDataSet.getValues().length);
        for (int i = 0; i < 5; i++) {
            double avg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
                    - (delStartTime + delEndTime) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod);
            if (getPathNum(avgResPaths.get(i)) >= threadNum){
                assertEquals(getPathNum(avgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0, (double)avgResult[i], delta);
            } else {
                assertEquals(avg + getPathNum(avgResPaths.get(i)), (double)avgResult[i], delta);
            }
        }
    }

    @Test
    public void sessionForTimePartDeleteTest() throws ExecutionException, SessionException, InterruptedException {
        insertRecords();

        //TODO 观察数据是否已经被插入完毕,和去掉这个sleep之后是否有区别
        Thread.sleep(10000);

        SessionQueryDataSet dataSet1 = session.queryData(paths, START_TIME, END_TIME + 1);
        int len1 = dataSet1.getTimestamps().length;
        List<String> resPaths1 = dataSet1.getPaths();
        assertEquals(5, resPaths1.size());
        assertEquals(TIME_PERIOD, len1);
        assertEquals(TIME_PERIOD, dataSet1.getValues().size());

        List<String> delPath = new LinkedList<>();
        delPath.add(COLUMN_D1_S1);
        delPath.add(COLUMN_D2_S2);
        delPath.add(COLUMN_D3_S3);
        delPath.add(COLUMN_D4_S4);

        int threadNum = 1;
        long delStartTime = START_TIME + TIME_PERIOD / 5;
        long delStep = TIME_PERIOD / 10;
        long delTimePeriod = delStep * threadNum;

        Task[] tasks = new Task[threadNum];
        Thread[] threads = new Thread[threadNum];
        long delEndTime = delStartTime + TIME_PERIOD / 10 * threadNum - 1;

        for (int i = 0; i < threadNum; i++) {
            tasks[i] = new Task(2, delPath, delStartTime + delStep * i,
                    delStartTime + delStep * (i + 1) - 1, delStep, 1);
            threads[i] = new Thread(tasks[i]);
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].join();
        }

        Thread.sleep(5000);

        //query
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(5, resPaths.size());
        assertEquals(TIME_PERIOD, len);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());

        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 5; j++) {
                if (delStartTime <= timestamp && timestamp <= delEndTime) {
                    if (getPathNum(resPaths.get(j)) == 4){
                        assertEquals(timestamp + getPathNum(resPaths.get(j)), result.get(j));
                    } else {
                        assertNull(result.get(j));
                    }
                } else {
                    assertEquals(getPathNum(resPaths.get(j)) + timestamp, result.get(j));
                }
            }
        }

        // Test avg function
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(paths.size(), avgResPaths.size());
        assertEquals(paths.size(), avgDataSet.getValues().length);
        for (int i = 0; i < 5; i++) {
            double avg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
                    - (delStartTime + delEndTime) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod);
            if (getPathNum(avgResPaths.get(i)) == 4){
                assertEquals(getPathNum(avgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0, (double)avgResult[i], delta);
            } else {
                assertEquals(avg + getPathNum(avgResPaths.get(i)), (double)avgResult[i], delta);
            }
        }
    }

    @Test
    public void sessionForStorageAllDeleteTest() throws ExecutionException, SessionException, InterruptedException {
        insertRecords();

        //TODO 观察数据是否已经被插入完毕,和去掉这个sleep之后是否有区别
        Thread.sleep(10000);

        long delStartTime = START_TIME;
        long delEndTime = END_TIME;
        long delTimePeriod = delEndTime - delStartTime + 1;

        // threadNum must < 5
        int threadNum = 1;
        Task[] tasks = new Task[threadNum];
        Thread[] threads = new Thread[threadNum];


        for (int i = 0; i < threadNum; i++) {
            tasks[i] = new Task(2, getSinglePathList(i), delStartTime, delEndTime, delTimePeriod, 1);
            threads[i] = new Thread(tasks[i]);
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].join();
        }

        Thread.sleep(5000);

        //query
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(5, resPaths.size());
        assertEquals(TIME_PERIOD, len);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());

        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 5; j++) {
                if (getPathNum(resPaths.get(j)) >= threadNum){
                    assertEquals(timestamp + getPathNum(resPaths.get(j)), result.get(j));
                } else {
                    assertNull(result.get(j));
                }
            }
        }

        // Test avg function
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(paths.size(), avgResPaths.size());
        assertEquals(paths.size(), avgDataSet.getValues().length);
        for (int i = 0; i < 5; i++) {
            if (getPathNum(avgResPaths.get(i)) >= threadNum){
                assertEquals(getPathNum(avgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0, (double)avgResult[i], delta);
            } else {
                assertEquals("null", new String((byte[]) avgResult[i]));
            }
        }
    }

    @Test
    public void sessionForTimeAllDeleteTest() throws ExecutionException, SessionException, InterruptedException {
        insertRecords();

        //TODO 观察数据是否已经被插入完毕,和去掉这个sleep之后是否有区别
        Thread.sleep(10000);

        List<String> delPath = new LinkedList<>();
        delPath.add(COLUMN_D1_S1);
        delPath.add(COLUMN_D2_S2);
        delPath.add(COLUMN_D3_S3);
        delPath.add(COLUMN_D4_S4);

        int threadNum = 1;
        long delStartTime = START_TIME;
        long delStep = TIME_PERIOD / threadNum;

        Task[] tasks = new Task[threadNum];
        Thread[] threads = new Thread[threadNum];

        for (int i = 0; i < threadNum; i++) {
            tasks[i] = new Task(2, delPath, delStartTime + delStep * i,
                    delStartTime + delStep * (i + 1) - 1, delStep, 1);
            threads[i] = new Thread(tasks[i]);
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i].join();
        }

        Thread.sleep(5000);

        //query
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(5, resPaths.size());
        assertEquals(TIME_PERIOD, len);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());

        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 5; j++) {
                if (getPathNum(resPaths.get(j)) == 4){
                    assertEquals(timestamp + getPathNum(resPaths.get(j)), result.get(j));
                } else {
                    assertNull(result.get(j));
                }
            }
        }

        // Test avg function
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(paths.size(), avgResPaths.size());
        assertEquals(paths.size(), avgDataSet.getValues().length);
        for (int i = 0; i < 5; i++) {
            if (getPathNum(avgResPaths.get(i)) == 4){
                assertEquals(getPathNum(avgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0, (double)avgResult[i], delta);
            } else {
                assertEquals("null", new String((byte[]) avgResult[i]));
            }
        }
    }

    private static class Task implements Runnable {

        //1:insert 2:delete
        private int type;
        private long startTime;
        private long endTime;
        private long pointNum;
        private int step;
        private List<String> path;

        private Session localSession;

        public Task(int type, List<String> path, long startTime, long endTime, long pointNum, int step) throws SessionException {
            this.type = type;
            this.path = path;
            this.pointNum = pointNum;
            this.step = step;
            this.startTime = startTime;
            this.endTime = endTime;
            this.localSession = new Session("127.0.0.1", 6888, "root", "root");
            this.localSession.openSession();
        }

        @Override
        public void run() {
            switch (type){
                //insert
                case 1:
                    long[] timestamps = new long[(int)pointNum];
                    for (long i = 0; i < pointNum; i++) {
                        timestamps[(int) i] = startTime + step * i;
                    }
                    int pathSize = path.size();
                    Object[] valuesList = new Object[pathSize];
                    for (int i = 0; i < pathSize; i++) {
                        Object[] values = new Object[(int)pointNum];
                        for (int j = 0; j < pointNum; j++) {
                            values[j] = timestamps[j] + getPathNum(path.get(i));
                        }
                        valuesList[i] = values;
                    }
                    List<DataType> dataTypeList = new ArrayList<>();
                    for (int i = 0; i < pathSize; i++) {
                        dataTypeList.add(DataType.LONG);
                    }
                    try {
                        localSession.insertColumnRecords(path, timestamps, valuesList, dataTypeList, null);
                    } catch (SessionException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    break;
                // delete
                case 2:
                    try {
                        session.deleteDataInColumns(path, startTime, endTime);
                    } catch(SessionException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    break;
                default:
                    break;
            }
        }
    }

    private static void addColumns() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(COLUMN_D1_S1);
        paths.add(COLUMN_D2_S2);
        paths.add(COLUMN_D3_S3);
        paths.add(COLUMN_D4_S4);
        paths.add(COLUMN_D5_S5);

        Map<String, String> attributesForOnePath = new HashMap<>();
        // INT64
        attributesForOnePath.put("DataType", "2");
        // RLE
        attributesForOnePath.put("Encoding", "2");
        // SNAPPY
        attributesForOnePath.put("Compression", "1");

        List<Map<String, String>> attributes = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            attributes.add(attributesForOnePath);
        }

        session.addColumns(paths, attributes);
    }

    private static int getPathNum(String sg){
        switch (sg){
            case COLUMN_D1_S1:
                return 0;
            case COLUMN_D2_S2:
                return 1;
            case COLUMN_D3_S3:
                return 2;
            case COLUMN_D4_S4:
                return 3;
            case COLUMN_D5_S5:
                return 4;
            default:
                return -1;
        }
    }

    private static List<String> getSinglePathList(int num){
        List<String> path = new LinkedList<>();
        switch(num){
            case 0:
                path.add(COLUMN_D1_S1);
                break;
            case 1:
                path.add(COLUMN_D2_S2);
                break;
            case 2:
                path.add(COLUMN_D3_S3);
                break;
            case 3:
                path.add(COLUMN_D4_S4);
                break;
            case 4:
                path.add(COLUMN_D5_S5);
                break;
            default:
                break;
        }
        return path;
    }

    private static void insertRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(COLUMN_D1_S1);
        paths.add(COLUMN_D2_S2);
        paths.add(COLUMN_D3_S3);
        paths.add(COLUMN_D4_S4);
        paths.add(COLUMN_D5_S5);

        for (int k = 0; k < 10; k++) {
            long[] timestamps = new long[(int)TIME_PERIOD];
            for (long i = 0; i < TIME_PERIOD; i++) {
                timestamps[(int) i] = i;
            }

            Object[] valuesList = new Object[5];
            for (int i = 0; i < 5; i++) {
                Object[] values = new Object[(int)TIME_PERIOD];
                for (long j = 0; j < TIME_PERIOD; j++) {
                    values[(int)j] = j + i;
                }
                valuesList[i] = values;
            }

            List<DataType> dataTypeList = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                dataTypeList.add(DataType.LONG);
            }

            session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
        }

    }

    private static void deleteDataInColumns() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(COLUMN_D1_S1);
        paths.add(COLUMN_D3_S3);
        paths.add(COLUMN_D4_S4);

        long startTime = 25L;
        long endTime = 30L;

        session.deleteDataInColumns(paths, startTime, endTime);
    }

}

