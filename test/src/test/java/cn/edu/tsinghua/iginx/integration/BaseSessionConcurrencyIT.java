package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;

import static org.junit.Assert.*;

public abstract class BaseSessionConcurrencyIT {

    //parameters to be flexibly configured by inheritance
    protected static MultiConnection session;
    protected boolean ifClearData = true;

    //host info
    protected String defaultTestHost = "127.0.0.1";
    protected int defaultTestPort = 6888;
    protected String defaultTestUser = "root";
    protected String defaultTestPass = "root";

    protected boolean isAbleToDelete;

    //original variables
    protected static final double delta = 1e-7;
    protected static final long TIME_PERIOD = 100000L;
    protected static final long START_TIME = 1000L;
    protected static final long END_TIME = START_TIME + TIME_PERIOD - 1;
    //params for partialDelete
    protected long delStartTime = START_TIME + TIME_PERIOD / 5;
    protected long delEndTime = START_TIME + TIME_PERIOD / 10 * 9;
    protected long delTimePeriod = delEndTime - delStartTime;
    protected double deleteAvg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
            - (delStartTime + delEndTime - 1) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod);

    protected int currPath = 0;

    protected static final Logger logger = LoggerFactory.getLogger(BaseSessionIT.class);

    @Before
    public void setUp() {
        try {
            session = new MultiConnection (new Session(defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass));
            session.openSession();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @After
    public void tearDown() throws SessionException {
        if(!ifClearData) return;

        try {
            clearData();
            session.closeSession();
        } catch (ExecutionException e) {
            logger.error(e.getMessage());
        }
    }

    protected void clearData() throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = session.executeSql(clearData);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Clear date execute fail. Caused by: {}.", res.getParseErrorMsg());
            fail();
        }
    }

    //TODO: a very suspicious test; somebody should do something
    //TODO: The following test must be added after bug fix
    //@Test
    public void multiThreadTestBad() throws SessionException, InterruptedException, ExecutionException {
        //query test, multithread insert for storage; multithread query
        int mulStQueryLen = 5;
        List<String> mulStPaths = getPaths(currPath, mulStQueryLen);
        BaseSessionConcurrencyIT.MultiThreadTask[] mulStInsertTasks = new BaseSessionConcurrencyIT.MultiThreadTask[mulStQueryLen];
        Thread[] mulStInsertThreads = new Thread[mulStQueryLen];
        for (int i = 0; i < mulStQueryLen; i++) {
            mulStInsertTasks[i] = new BaseSessionConcurrencyIT.MultiThreadTask(1, getPaths(currPath + i, 1), START_TIME, END_TIME,
                    TIME_PERIOD, 1, null, 6888);
            mulStInsertThreads[i] = new Thread(mulStInsertTasks[i]);
        }
        for (int i = 0; i < mulStQueryLen; i++) {
            mulStInsertThreads[i].start();
        }
        for (int i = 0; i < mulStQueryLen; i++) {
            mulStInsertThreads[i].join();
        }
        Thread.sleep(1000);

        //query
        int queryTaskNum = 4;
        BaseSessionConcurrencyIT.MultiThreadTask[] mulStQueryTasks = new BaseSessionConcurrencyIT.MultiThreadTask[queryTaskNum];
        Thread[] mulStQueryThreads = new Thread[queryTaskNum];
        //each query query one storage

        for (int i = 0; i < queryTaskNum; i++) {
            mulStQueryTasks[i] = new BaseSessionConcurrencyIT.MultiThreadTask(3, mulStPaths, START_TIME, END_TIME + 1,
                    0, 0, null, 6888);
            mulStQueryThreads[i] = new Thread(mulStQueryTasks[i]);
        }
        for (int i = 0; i < queryTaskNum; i++) {
            mulStQueryThreads[i].start();
        }
        for (int i = 0; i < queryTaskNum; i++) {
            mulStQueryThreads[i].join();
        }
        Thread.sleep(3000);
        // TODO change the simple query and one of the avg query to multithread
        try {
            for (int i = 0; i < queryTaskNum; i++) {
                SessionQueryDataSet dataSet = (SessionQueryDataSet) mulStQueryTasks[i].getQueryDataSet();
                int len = dataSet.getKeys().length;
                List<String> resPaths = dataSet.getPaths();
                assertEquals(mulStQueryLen, resPaths.size());
                assertEquals(TIME_PERIOD, len);
                assertEquals(TIME_PERIOD, dataSet.getValues().size());
                for (int j = 0; j < len; j++) {
                    long timestamp = dataSet.getKeys()[j];
                    assertEquals(j + START_TIME, timestamp);
                    List<Object> result = dataSet.getValues().get(j);
                    for (int k = 0; k < mulStQueryLen; k++) {
                        assertEquals(getPathNum(resPaths.get(k)) + timestamp, result.get(k));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        // Test max function
        SessionAggregateQueryDataSet mulStMaxDataSet = session.aggregateQuery(mulStPaths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> mulStMaxResPaths = mulStMaxDataSet.getPaths();
        Object[] mulStMaxResult = mulStMaxDataSet.getValues();
        assertEquals(mulStQueryLen, mulStMaxResPaths.size());
        assertEquals(mulStQueryLen, mulStMaxDataSet.getValues().length);
        for (int i = 0; i < mulStQueryLen; i++) {
            assertEquals(getPathNum(mulStMaxResPaths.get(i)) + END_TIME, mulStMaxResult[i]);
        }
        // Test avg function
        SessionAggregateQueryDataSet mulStAvgDataSet = session.aggregateQuery(mulStPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> mulStAvgResPaths = mulStAvgDataSet.getPaths();
        Object[] mulStAvgResult = mulStAvgDataSet.getValues();
        assertEquals(mulStQueryLen, mulStAvgResPaths.size());
        assertEquals(mulStQueryLen, mulStAvgDataSet.getValues().length);
        for (int i = 0; i < mulStQueryLen; i++) {
            assertEquals(getPathNum(mulStAvgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0, changeResultToDouble(mulStAvgResult[i]), delta);
        }
        currPath += mulStQueryLen;
        //query test, multithread insert for time, multithread query
        int mulTimeQueryLen = 5;
        List<String> mulTimePaths = getPaths(currPath, mulTimeQueryLen);
        BaseSessionConcurrencyIT.MultiThreadTask[] mulTimeInsertTasks = new BaseSessionConcurrencyIT.MultiThreadTask[mulTimeQueryLen];
        Thread[] mulTimeInsertThreads = new Thread[mulTimeQueryLen];
        for (int i = 0; i < mulTimeQueryLen; i++) {
            mulTimeInsertTasks[i] = new BaseSessionConcurrencyIT.MultiThreadTask(1, mulTimePaths, START_TIME + i, END_TIME - (4 - i),
                    TIME_PERIOD / mulTimeQueryLen, mulTimeQueryLen, null, 6888);
            mulTimeInsertThreads[i] = new Thread(mulTimeInsertTasks[i]);
        }
        for (int i = 0; i < mulTimeQueryLen; i++) {
            mulTimeInsertThreads[i].start();
        }
        for (int i = 0; i < mulTimeQueryLen; i++) {
            mulTimeInsertThreads[i].join();
        }
        Thread.sleep(1000);
        //query
        // TODO change the simple query and one of the avg query to multithread
        SessionQueryDataSet mulTimeQueryDataSet = session.queryData(mulTimePaths, START_TIME, END_TIME + 1);
        int mulTimeResLen = mulTimeQueryDataSet.getKeys().length;
        List<String> mulTimeQueryResPaths = mulTimeQueryDataSet.getPaths();
        assertEquals(mulTimeQueryLen, mulTimeQueryResPaths.size());
        assertEquals(TIME_PERIOD, mulTimeResLen);
        assertEquals(TIME_PERIOD, mulTimeQueryDataSet.getValues().size());
        for (int i = 0; i < mulTimeResLen; i++) {
            long timestamp = mulTimeQueryDataSet.getKeys()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = mulTimeQueryDataSet.getValues().get(i);
            for (int j = 0; j < mulTimeQueryLen; j++) {
                assertEquals(getPathNum(mulTimeQueryResPaths.get(j)) + timestamp, result.get(j));
            }
        }

        // Test max function
        SessionAggregateQueryDataSet mulTimeMaxDataSet = session.aggregateQuery(mulTimePaths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> mulTimeMaxResPaths = mulTimeMaxDataSet.getPaths();
        Object[] mulTimeMaxResult = mulTimeMaxDataSet.getValues();
        assertEquals(mulTimeQueryLen, mulTimeMaxResPaths.size());
        assertEquals(mulTimeQueryLen, mulTimeMaxDataSet.getValues().length);
        for (int i = 0; i < mulTimeQueryLen; i++) {
            assertEquals(getPathNum(mulTimeMaxResPaths.get(i)) + END_TIME, mulTimeMaxResult[i]);
        }
        // Test avg function
        SessionAggregateQueryDataSet mulTimeAvgDataSet = session.aggregateQuery(mulTimePaths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> mulTimeAvgResPaths = mulTimeAvgDataSet.getPaths();
        Object[] mulTimeAvgResult = mulTimeAvgDataSet.getValues();
        assertEquals(mulTimeQueryLen, mulTimeAvgResPaths.size());
        assertEquals(mulTimeQueryLen, mulTimeAvgDataSet.getValues().length);
        for (int i = 0; i < mulTimeQueryLen; i++) {
            assertEquals(getPathNum(mulTimeAvgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0,
                    changeResultToDouble(mulTimeAvgResult[i]), delta);
        }
        currPath += mulTimeQueryLen;

        // multithread delete test, insert in
        if (isAbleToDelete) {
            // for Storage Part delete
            int mulDelPSLen = 5;
            List<String> mulDelPSPaths = getPaths(currPath, mulDelPSLen);
            insertNumRecords(mulDelPSPaths);
            Thread.sleep(1000);
            SessionQueryDataSet beforePSDataSet = session.queryData(mulDelPSPaths, START_TIME, END_TIME + 1);
            List<String> bfPSPath = beforePSDataSet.getPaths();
            assertEquals(mulDelPSLen, bfPSPath.size());
            assertEquals(TIME_PERIOD, beforePSDataSet.getValues().size());
            int delPSThreadNum = mulDelPSLen - 1;
            BaseSessionConcurrencyIT.MultiThreadTask[] delPSTasks = new BaseSessionConcurrencyIT.MultiThreadTask[delPSThreadNum];
            Thread[] delPSThreads = new Thread[delPSThreadNum];
            for (int i = 0; i < delPSThreadNum; i++) {
                delPSTasks[i] = new BaseSessionConcurrencyIT.MultiThreadTask(2, getPaths(currPath + i, 1), delStartTime,
                        delEndTime, delTimePeriod, 1, null, 6888);
                delPSThreads[i] = new Thread(delPSTasks[i]);
            }
            for (int i = 0; i < delPSThreadNum; i++) {
                delPSThreads[i].start();
            }
            for (int i = 0; i < delPSThreadNum; i++) {
                delPSThreads[i].join();
            }
            Thread.sleep(1000);

            //query
            SessionQueryDataSet delPSDataSet = session.queryData(mulDelPSPaths, START_TIME, END_TIME + 1);
            int delPSQueryLen = delPSDataSet.getKeys().length;
            List<String> delPSResPaths = delPSDataSet.getPaths();
            assertEquals(mulDelPSLen, delPSResPaths.size());
            assertEquals(TIME_PERIOD, delPSQueryLen);
            assertEquals(TIME_PERIOD, delPSDataSet.getValues().size());
            for (int i = 0; i < delPSQueryLen; i++) {
                long timestamp = delPSDataSet.getKeys()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = delPSDataSet.getValues().get(i);
                for (int j = 0; j < mulDelPSLen; j++) {
                    if (delStartTime <= timestamp && timestamp < delEndTime) {
                        if (getPathNum(delPSResPaths.get(j)) >= currPath + delPSThreadNum) {
                            assertEquals(timestamp + getPathNum(delPSResPaths.get(j)), result.get(j));
                        } else {
                            assertNull(result.get(j));
                        }
                    } else {
                        assertEquals(getPathNum(delPSResPaths.get(j)) + timestamp, result.get(j));
                    }
                }
            }

            // Test avg function
            SessionAggregateQueryDataSet delPSAvgDataSet = session.aggregateQuery(mulDelPSPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> delPSAvgResPaths = delPSAvgDataSet.getPaths();
            Object[] delPSAvgResult = delPSAvgDataSet.getValues();
            assertEquals(mulDelPSLen, delPSAvgResPaths.size());
            assertEquals(mulDelPSLen, delPSAvgDataSet.getValues().length);
            for (int i = 0; i < mulDelPSLen; i++) {
                double avg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
                        - (delStartTime + delEndTime - 1) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod);
                if (getPathNum(delPSAvgResPaths.get(i)) >= currPath + delPSThreadNum) {
                    assertEquals(getPathNum(delPSAvgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0,
                            changeResultToDouble(delPSAvgResult[i]), delta);
                } else {
                    assertEquals(avg + getPathNum(delPSAvgResPaths.get(i)),
                            changeResultToDouble(delPSAvgResult[i]), delta);
                }
            }

            currPath += mulDelPSLen;

            // for Time Part delete
            int mulDelPTLen = 5;
            List<String> mulDelPTPaths = getPaths(currPath, mulDelPTLen);
            insertNumRecords(mulDelPTPaths);
            Thread.sleep(1000);
            SessionQueryDataSet beforePTDataSet = session.queryData(mulDelPTPaths, START_TIME, END_TIME + 1);
            int beforePTLen = beforePTDataSet.getKeys().length;
            List<String> beforePTPaths = beforePTDataSet.getPaths();
            assertEquals(mulDelPTLen, beforePTPaths.size());
            assertEquals(TIME_PERIOD, beforePTLen);
            assertEquals(TIME_PERIOD, beforePTDataSet.getValues().size());
            int delPTThreadNum = 5;
            int delPTPathNum = 4;
            // the deleted paths of the data
            List<String> delPTPaths = getPaths(currPath, delPTPathNum);
            BaseSessionConcurrencyIT.MultiThreadTask[] delPTTasks = new BaseSessionConcurrencyIT.MultiThreadTask[delPTThreadNum];
            Thread[] delPTThreads = new Thread[delPTThreadNum];
            long delPTStartTime = START_TIME + TIME_PERIOD / 5;
            long delPTStep = TIME_PERIOD / 10;
            long delPTTimePeriod = delPTStep * delPTThreadNum;
            long delPTEndTime = delPTStartTime + TIME_PERIOD / 10 * delPTThreadNum - 1;
            for (int i = 0; i < delPTThreadNum; i++) {
                delPTTasks[i] = new BaseSessionConcurrencyIT.MultiThreadTask(2, delPTPaths, delPTStartTime + delPTStep * i,
                        delPTStartTime + delPTStep * (i + 1), delPTStep, 1, null, 6888);
                delPTThreads[i] = new Thread(delPTTasks[i]);
            }
            for (int i = 0; i < delPTThreadNum; i++) {
                delPTThreads[i].start();
            }
            for (int i = 0; i < delPTThreadNum; i++) {
                delPTThreads[i].join();
            }
            Thread.sleep(1000);

            //query
            SessionQueryDataSet delPTDataSet = session.queryData(mulDelPTPaths, START_TIME, END_TIME + 1);
            int delPTQueryLen = delPTDataSet.getKeys().length;
            List<String> delPTResPaths = delPTDataSet.getPaths();
            assertEquals(mulDelPTLen, delPTResPaths.size());
            assertEquals(TIME_PERIOD, delPTQueryLen);
            assertEquals(TIME_PERIOD, delPTDataSet.getValues().size());
            for (int i = 0; i < delPTQueryLen; i++) {
                long timestamp = delPTDataSet.getKeys()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = delPTDataSet.getValues().get(i);
                for (int j = 0; j < mulDelPTLen; j++) {
                    if (delPTStartTime <= timestamp && timestamp <= delPTEndTime) {
                        if (getPathNum(delPTResPaths.get(j)) >= currPath + delPTPathNum) {
                            assertEquals(timestamp + getPathNum(delPTResPaths.get(j)), result.get(j));
                        } else {
                            assertNull(result.get(j));
                        }
                    } else {
                        assertEquals(getPathNum(delPTResPaths.get(j)) + timestamp, result.get(j));
                    }
                }
            }
            // Test avg function
            SessionAggregateQueryDataSet delPTAvgDataSet = session.aggregateQuery(mulDelPTPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> delPTAvgResPaths = delPTAvgDataSet.getPaths();
            Object[] delPTAvgResult = delPTAvgDataSet.getValues();
            assertEquals(mulDelPTLen, delPTAvgResPaths.size());
            assertEquals(mulDelPTLen, delPTAvgDataSet.getValues().length);
            for (int i = 0; i < mulDelPTLen; i++) {
                double avg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
                        - (delPTStartTime + delPTEndTime) * delPTTimePeriod / 2.0) / (TIME_PERIOD - delPTTimePeriod);
                if (getPathNum(delPTAvgResPaths.get(i)) >= currPath + delPTPathNum) {
                    assertEquals(getPathNum(delPTAvgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0,
                            changeResultToDouble(delPTAvgResult[i]), delta);
                } else {
                    assertEquals(avg + getPathNum(delPTAvgResPaths.get(i)),
                            changeResultToDouble(delPTAvgResult[i]), delta);
                }
            }
            currPath += mulDelPTLen;

            // for Storage All delete
            int mulDelASLen = 5;
            List<String> mulDelASPaths = getPaths(currPath, mulDelASLen);
            insertNumRecords(mulDelASPaths);
            Thread.sleep(1000);
            // threadNum must < 5
            int delASThreadNum = 4;
            BaseSessionConcurrencyIT.MultiThreadTask[] delASTasks = new BaseSessionConcurrencyIT.MultiThreadTask[delASThreadNum];
            Thread[] delASThreads = new Thread[delASThreadNum];
            for (int i = 0; i < delASThreadNum; i++) {
                delASTasks[i] = new BaseSessionConcurrencyIT.MultiThreadTask(2, getPaths(currPath + i, 1), START_TIME,
                        END_TIME + 1, TIME_PERIOD, 1, null, 6888);
                delASThreads[i] = new Thread(delASTasks[i]);
            }
            for (int i = 0; i < delASThreadNum; i++) {
                delASThreads[i].start();
            }
            for (int i = 0; i < delASThreadNum; i++) {
                delASThreads[i].join();
            }
            Thread.sleep(1000);
            //query
            SessionQueryDataSet delASDataSet = session.queryData(mulDelASPaths, START_TIME, END_TIME + 1);
            int delASLen = delASDataSet.getKeys().length;
            List<String> delASResPaths = delASDataSet.getPaths();
            assertEquals(mulDelASLen, delASResPaths.size());
            assertEquals(TIME_PERIOD, delASLen);
            assertEquals(TIME_PERIOD, delASDataSet.getValues().size());

            for (int i = 0; i < delASLen; i++) {
                long timestamp = delASDataSet.getKeys()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = delASDataSet.getValues().get(i);
                for (int j = 0; j < mulDelASLen; j++) {
                    if (getPathNum(delASResPaths.get(j)) >= currPath + delASThreadNum) {
                        assertEquals(timestamp + getPathNum(delASResPaths.get(j)), result.get(j));
                    } else {
                        assertNull(result.get(j));
                    }
                }
            }

            // Test avg function
            SessionAggregateQueryDataSet delASAvgDataSet = session.aggregateQuery(mulDelASPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> delASAvgResPaths = delASAvgDataSet.getPaths();
            Object[] delASAvgResult = delASAvgDataSet.getValues();
            assertEquals(mulDelASLen, delASAvgResPaths.size());
            assertEquals(mulDelASLen, delASAvgDataSet.getValues().length);
            for (int i = 0; i < mulDelASLen; i++) {
                if (getPathNum(delASAvgResPaths.get(i)) >= currPath + delASThreadNum) {
                    assertEquals(getPathNum(delASAvgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0,
                            changeResultToDouble(delASAvgResult[i]), delta);
                } else {
//                    assertEquals("null", new String((byte[]) delASAvgResult[i]));
                    assertTrue(Double.isNaN((Double) delASAvgResult[i]));
                }
            }
            currPath += mulDelASLen;

            // for time All delete
            int mulDelATLen = 5;
            List<String> mulDelATPaths = getPaths(currPath, mulDelATLen);
            insertNumRecords(mulDelATPaths);
            //Thread.sleep(1000);
            int delATPathLen = 4;
            List<String> delATPath = getPaths(currPath, delATPathLen);
            int delATThreadNum = 5;
            long delATStartTime = START_TIME;
            long delATStep = TIME_PERIOD / delATThreadNum;

            BaseSessionConcurrencyIT.MultiThreadTask[] delATTasks = new BaseSessionConcurrencyIT.MultiThreadTask[delATThreadNum];
            Thread[] delATThreads = new Thread[delATThreadNum];

            for (int i = 0; i < delATThreadNum; i++) {
                delATTasks[i] = new BaseSessionConcurrencyIT.MultiThreadTask(2, delATPath, delATStartTime + delATStep * i,
                        delATStartTime + delATStep * (i + 1), delATStep, 1, null, 6888);
                delATThreads[i] = new Thread(delATTasks[i]);
            }
            for (int i = 0; i < delATThreadNum; i++) {
                delATThreads[i].start();
            }
            for (int i = 0; i < delATThreadNum; i++) {
                delATThreads[i].join();
            }
            Thread.sleep(1000);
            //query
            SessionQueryDataSet delATDataSet = session.queryData(mulDelATPaths, START_TIME, END_TIME + 1);
            int delATLen = delATDataSet.getKeys().length;
            List<String> delATResPaths = delATDataSet.getPaths();
            assertEquals(mulDelATLen, delATResPaths.size());
            assertEquals(TIME_PERIOD, delATLen);
            assertEquals(TIME_PERIOD, delATDataSet.getValues().size());
            for (int i = 0; i < delATLen; i++) {
                long timestamp = delATDataSet.getKeys()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = delATDataSet.getValues().get(i);
                for (int j = 0; j < mulDelATLen; j++) {
                    if (getPathNum(delATResPaths.get(j)) >= currPath + delATPathLen) {
                        assertEquals(timestamp + getPathNum(delATResPaths.get(j)), result.get(j));
                    } else {
                        assertNull(result.get(j));
                    }
                }
            }

            // Test avg function
            SessionAggregateQueryDataSet delATAvgDataSet = session.aggregateQuery(mulDelATPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> delATAvgResPaths = delATAvgDataSet.getPaths();
            Object[] delATAvgResult = delATAvgDataSet.getValues();
            assertEquals(mulDelATLen, delATAvgResPaths.size());
            assertEquals(mulDelATLen, delATAvgDataSet.getValues().length);
            for (int i = 0; i < mulDelATLen; i++) {
                if (getPathNum(delATAvgResPaths.get(i)) >= currPath + delATPathLen) {
                    assertEquals(getPathNum(delATAvgResPaths.get(i)) + (START_TIME + END_TIME) / 2.0,
                            changeResultToDouble(delATAvgResult[i]), delta);
                } else {
//                    assertEquals("null", new String((byte[]) delATAvgResult[i]));
                    assertTrue(Double.isNaN((Double) delATAvgResult[i]));
                }
            }

            currPath += mulDelATLen;
        }
    }

    protected int getPathNum(String path) {
        if (path.contains("(") && path.contains(")")) {
            path = path.substring(path.indexOf("(") + 1, path.indexOf(")"));
        }

        String pattern = "^sg1\\.d(\\d+)\\.s(\\d+)$";
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(path);
        if (m.find()) {
            int d = Integer.parseInt(m.group(1));
            int s = Integer.parseInt(m.group(2));
            if (d == s) {
                return d;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    protected List<String> getPaths(int startPosition, int len) {
        List<String> paths = new ArrayList<>();
        for (int i = startPosition; i < startPosition + len; i++) {
            paths.add("sg1.d" + i + ".s" + i);
        }
        return paths;
    }

    protected double changeResultToDouble(Object rawResult) {
        double result = 0;
        if (rawResult instanceof java.lang.Long) {
            result = (double) ((long) rawResult);
        } else {
            try {
                result = (double) rawResult;
            } catch (Exception e) {
                logger.error(e.getMessage());
                fail();
            }
        }
        return result;
    }

    protected void insertNumRecords(List<String> insertPaths) throws SessionException, ExecutionException {
        int pathLen = insertPaths.size();
        long[] timestamps = new long[(int) TIME_PERIOD];
        for (long i = 0; i < TIME_PERIOD; i++) {
            timestamps[(int) i] = i + START_TIME;
        }

        Object[] valuesList = new Object[pathLen];
        for (int i = 0; i < pathLen; i++) {
            int pathNum = getPathNum(insertPaths.get(i));
            Object[] values = new Object[(int) TIME_PERIOD];
            for (long j = 0; j < TIME_PERIOD; j++) {
                values[(int) j] = pathNum + j + START_TIME;
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < pathLen; i++) {
            dataTypeList.add(DataType.LONG);
        }
        session.insertNonAlignedColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
    }

    protected class MultiThreadTask implements Runnable {

        //1:insert 2:delete 3:query
        private int type;
        private long startTime;
        private long endTime;
        private long pointNum;
        private int step;
        private List<String> path;
        private Object queryDataSet;
        private AggregateType aggregateType;

        private MultiConnection localSession;

        public MultiThreadTask(int type, List<String> path, long startTime, long endTime,
                               long pointNum, int step, AggregateType aggrType, int portNum) throws SessionException {
            this.type = type;
            this.path = new ArrayList(path);
            this.startTime = startTime;
            this.endTime = endTime;
            this.pointNum = pointNum;
            this.step = step;
            this.queryDataSet = null;
            this.aggregateType = aggrType;

            this.localSession = session.isSession()?new MultiConnection( new Session(defaultTestHost, defaultTestPort,
                    defaultTestUser, defaultTestPass) ):session;
            this.localSession.openSession();
        }

        @Override
        public void run() {
            switch (type) {
                //insert
                case 1:
                    long[] timestamps = new long[(int) pointNum];
                    for (long i = 0; i < pointNum; i++) {
                        timestamps[(int) i] = startTime + step * i;
                    }
                    int pathSize = path.size();
                    Object[] valuesList = new Object[pathSize];
                    for (int i = 0; i < pathSize; i++) {
                        Object[] values = new Object[(int) pointNum];
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
                        localSession.insertNonAlignedColumnRecords(path, timestamps, valuesList, dataTypeList, null);
                    } catch (SessionException | ExecutionException e) {
                        logger.error(e.getMessage());
                    }
                    break;
                // delete
                case 2:
                    try {
                        localSession.deleteDataInColumns(path, startTime, endTime);
                    } catch (SessionException | ExecutionException e) {
                        logger.error(e.getMessage());
                    }
                    break;
                //query
                case 3:
                    try {
                        if (aggregateType == null) {
                            queryDataSet = localSession.queryData(path, startTime, endTime);
                        } else {
                            queryDataSet = localSession.aggregateQuery(path, startTime, endTime, aggregateType);
                        }
                    } catch (SessionException | ExecutionException e) {
                        logger.error(e.getMessage());
                    }
                    break;
                default:
                    break;
            }
            try {
                if(localSession.isSession())
                    this.localSession.closeSession();
            } catch (SessionException e) {
                logger.error(e.getMessage());
            }
        }

        public Object getQueryDataSet() {
            return queryDataSet;
        }
    }
}
