/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public abstract class BaseSessionIT {

    //parameters to be flexibly configured by inheritance
    protected static MultiConnection session;
    protected boolean ifClearData = true;

    protected boolean isAbleToDelete;
    protected String storageEngineType;
    protected int defaultPort2;
    protected Map<String, String> extraParams;

    //host info
    protected String defaultTestHost = "127.0.0.1";
    protected int defaultTestPort = 6888;
    protected String defaultTestUser = "root";
    protected String defaultTestPass = "root";

    //original variables
    protected static final Logger logger = LoggerFactory.getLogger(BaseSessionIT.class);
    private static final long TIME_PERIOD = 100000L;
    private static final long START_TIME = 1000L;
    private static final long END_TIME = START_TIME + TIME_PERIOD - 1;
    private static final double delta = 1e-7;
    //params for downSample
    private static final long PRECISION = 123L;
    //params for datatype test
    private static final String ranStr = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final int STRING_LEN = 1000;

    long factSampleLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
    double originAvg = (START_TIME + END_TIME) / 2.0;
    private int currPath = 0;
    //params for partialDelete
    private long delStartTime = START_TIME + TIME_PERIOD / 5;
    private long delEndTime = START_TIME + TIME_PERIOD / 10 * 9;
    private long delTimePeriod = delEndTime - delStartTime;
    double deleteAvg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
        - (delStartTime + delEndTime - 1) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod);

    private List<String> getPaths(int startPosition, int len) {
        List<String> paths = new ArrayList<>();
        for (int i = startPosition; i < startPosition + len; i++) {
            paths.add("sg1.d" + i + ".s" + i);
        }
        return paths;
    }

    private String getSinglePath(int startPosition, int offset) {
        int pos = startPosition + offset;
        return ("sg1.d" + pos + ".s" + pos);
    }

    private int getPathNum(String path) {
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

    private String getRandomStr(int seed, int length) {
        Random random = new Random(seed);
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < length; k++) {
            int number = random.nextInt(ranStr.length());
            sb.append(ranStr.charAt(number));
        }
        return sb.toString();
    }

    private void insertTestsByFourInterfaces()throws SessionException, ExecutionException {
       CombinedInsertTests test = new CombinedInsertTests(session);
       test.testInserts();
    }

    private void insertNumRecords(List<String> insertPaths) throws SessionException, ExecutionException {
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

    private void insertFakeNumRecords(List<String> insertPaths, long count) throws SessionException, ExecutionException {
        int pathLen = insertPaths.size();
        long[] timestamps = new long[(int) TIME_PERIOD];
        for (long i = 0; i < TIME_PERIOD; i++) {
            timestamps[(int) i] = i + START_TIME + count;
        }

        Object[] valuesList = new Object[pathLen];
        for (int i = 0; i < pathLen; i++) {
            int pathNum = getPathNum(insertPaths.get(i));
            Object[] values = new Object[(int) TIME_PERIOD];
            for (long j = 0; j < TIME_PERIOD; j++) {
                if (i == 0) {
                    values[(int) j] = pathNum + j + START_TIME + 0.0001;
                } else {
                    values[(int) j] = pathNum + j + START_TIME;
                }
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < pathLen; i++) {
            if (i == 0) {
                dataTypeList.add(DataType.DOUBLE);
            } else {
                dataTypeList.add(DataType.LONG);
            }
        }
        session.insertNonAlignedColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
    }

    // the length of the insertPaths must be 6
    private void insertDataTypeRecords(List<String> insertPaths, int startPathNum) throws SessionException, ExecutionException {
        long[] timestamps = new long[(int) TIME_PERIOD];
        for (long i = 0; i < TIME_PERIOD; i++) {
            timestamps[(int) i] = i + START_TIME;
        }

        Object[] valuesList = new Object[6];
        for (int i = 0; i < 6; i++) {
            Object[] values = new Object[(int) TIME_PERIOD];
            int pathNum = getPathNum(insertPaths.get(i)) - startPathNum;
            for (int j = 0; j < TIME_PERIOD; j++) {
                switch (pathNum) {
                    case 1:
                        //integer
                        values[j] = (int) (pathNum + (TIME_PERIOD - j - 1) + START_TIME);
                        break;
                    case 2:
                        //long
                        values[j] = (pathNum + j + START_TIME) * 1000;
                        break;
                    case 3:
                        values[j] = (float) (pathNum + j + START_TIME + 0.01);
                        //float
                        break;
                    case 4:
                        //double
                        values[j] = (pathNum + (TIME_PERIOD - j - 1) + START_TIME + 0.01) * 999;
                        break;
                    case 5:
                        //binary
                        values[j] = getRandomStr(j, STRING_LEN).getBytes();
                        break;
                    default:
                        //boolean
                        values[j] = (j % 2 == 0);
                        break;
                }
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            dataTypeList.add(DataType.findByValue(i));
        }
        session.insertNonAlignedColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
    }

    private double changeResultToDouble(Object rawResult) {
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

    private float changeResultToFloat(Object rawResult) {
        float result = 0;
        if (rawResult instanceof java.lang.Double) {
            result = (float) ((double) rawResult);
        } else {
            try {
                result = (float) rawResult;
            } catch (Exception e) {
                logger.error(e.getMessage());
                fail();
            }
        }
        return result;
    }

    private int changeResultToInteger(Object rawResult) {
        int result = 0;
        if (rawResult instanceof java.lang.Long) {
            result = (int) ((long) rawResult);
        } else {
            try {
                result = (int) rawResult;
            } catch (Exception e) {
                logger.error(e.getMessage());
                fail();
            }
        }
        return result;
    }

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

    private void clearData() throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = session.executeSql(clearData);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Clear date execute fail. Caused by: {}.", res.getParseErrorMsg());
            fail();
        }
    }

    @Test
    public void iotdb11_IT() {
    }

    @Test
    public void iotdb12_IT() {
    }

    @Test
    public void capacityExpansion() throws ExecutionException, SessionException, InterruptedException {
        if(ifClearData) return;

        sessionTest();
    }

    @Test
    public void sessionTest() throws ExecutionException, SessionException, InterruptedException {
        int simpleLen = 2;
        List<String> paths = getPaths(currPath, simpleLen);
        //Simple Test(Including query,valueFilter,aggr:max/min/first/last/count/sum/avg)
        insertTestsByFourInterfaces();
        insertNumRecords(paths);
        //query
        SessionQueryDataSet simpleQueryDataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int simpleResLen = simpleQueryDataSet.getTimestamps().length;
        List<String> queryResPaths = simpleQueryDataSet.getPaths();
        assertEquals(simpleLen, queryResPaths.size());
        assertEquals(TIME_PERIOD, simpleResLen);
        assertEquals(TIME_PERIOD, simpleQueryDataSet.getValues().size());
        for (int i = 0; i < simpleResLen; i++) {
            long timestamp = simpleQueryDataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> queryResult = simpleQueryDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                int pathNum = getPathNum(queryResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(timestamp + pathNum, queryResult.get(j));
            }
        }
        // aggrMax
        SessionAggregateQueryDataSet maxDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> maxResPaths = maxDataSet.getPaths();
        Object[] maxResult = maxDataSet.getValues();
        assertEquals(simpleLen, maxResPaths.size());
//        assertEquals(simpleLen, maxDataSet.getTimestamps().length);
        assertEquals(simpleLen, maxDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            //assertEquals(-1, maxDataSet.getTimestamps()[i]);
            int pathNum = getPathNum(maxResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(END_TIME + pathNum, maxResult[i]);
        }
        // aggrMin
        SessionAggregateQueryDataSet minDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MIN);
        List<String> minResPaths = minDataSet.getPaths();
        Object[] minResult = minDataSet.getValues();
        assertEquals(simpleLen, minResPaths.size());
//        assertEquals(simpleLen, minDataSet.getTimestamps().length);
        assertEquals(simpleLen, minDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            //assertEquals(-1, minDataSet.getTimestamps()[i]);
            int pathNum = getPathNum(minResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(START_TIME + pathNum, minResult[i]);
        }
        //aggrFirst
        SessionAggregateQueryDataSet firstDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.FIRST_VALUE);
        List<String> firstResPaths = firstDataSet.getPaths();
        Object[] firstResult = firstDataSet.getValues();
        assertEquals(simpleLen, firstResPaths.size());
//        assertEquals(simpleLen, firstDataSet.getTimestamps().length);
        assertEquals(simpleLen, firstDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            int pathNum = getPathNum(firstResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(START_TIME + pathNum, firstResult[i]);
        }
        //aggrLast
        SessionAggregateQueryDataSet lastDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.LAST_VALUE);
        List<String> lastResPaths = lastDataSet.getPaths();
        Object[] lastResult = lastDataSet.getValues();
        assertEquals(simpleLen, lastResPaths.size());
//        assertEquals(simpleLen, lastDataSet.getTimestamps().length);
        assertEquals(simpleLen, lastDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            int pathNum = getPathNum(lastResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(END_TIME + pathNum, lastResult[i]);
        }
        //aggrCount
        SessionAggregateQueryDataSet countDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT);
        assertNull(countDataSet.getTimestamps());
        List<String> countResPaths = countDataSet.getPaths();
        Object[] countResult = countDataSet.getValues();
        assertEquals(simpleLen, countResPaths.size());
        assertEquals(simpleLen, countDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            assertEquals(TIME_PERIOD, countResult[i]);
        }
        //aggrSum
        SessionAggregateQueryDataSet sumDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.SUM);
        assertNull(sumDataSet.getTimestamps());
        List<String> sumResPaths = sumDataSet.getPaths();
        Object[] sumResult = sumDataSet.getValues();
        assertEquals(simpleLen, sumResPaths.size());
        assertEquals(simpleLen, sumDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            double sum = (START_TIME + END_TIME) * TIME_PERIOD / 2.0;
            int pathNum = getPathNum(sumResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(sum + pathNum * TIME_PERIOD, changeResultToDouble(sumResult[i]), delta);
        }

        //aggrAvg
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        assertNull(avgDataSet.getTimestamps());
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(simpleLen, avgResPaths.size());
        assertEquals(simpleLen, avgDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            double avg = (START_TIME + END_TIME) / 2.0;
            int pathNum = getPathNum(avgResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(avg + pathNum, changeResultToDouble(avgResult[i]), delta);
        }

        /*
        // downSample Aggregate
        //TODO the same aggregate return different value in iotdb and influxdb, unlocked this test until fixed
        // downSample max
        SessionQueryDataSet dsMaxDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX, PRECISION);
        int dsMaxLen = dsMaxDataSet.getTimestamps().length;
        List<String> dsMaxResPaths = dsMaxDataSet.getPaths();
        assertEquals(simpleLen, dsMaxResPaths.size());
        assertEquals(factSampleLen, dsMaxLen);
        assertEquals(factSampleLen, dsMaxDataSet.getValues().size());
        for (int i = 0; i < dsMaxLen; i++) {
            long dsTimestamp = dsMaxDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsMaxDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                int pathNum = getPathNum(dsMaxResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(maxNum + pathNum, dsResult.get(j));
            }
        }
        //min
        SessionQueryDataSet dsMinDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.MIN, PRECISION);
        int dsMinLen = dsMinDataSet.getTimestamps().length;
        List<String> dsMinResPaths = dsMinDataSet.getPaths();
        assertEquals(simpleLen, dsMinResPaths.size());
        assertEquals(factSampleLen, dsMinLen);
        assertEquals(factSampleLen, dsMinDataSet.getValues().size());
        for (int i = 0; i < dsMinLen; i++) {
            long dsTimestamp = dsMinDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsMinDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long minNum = START_TIME + i * PRECISION;
                int pathNum = getPathNum(dsMinResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(minNum + pathNum, dsResult.get(j));
            }
        }
        //first
        SessionQueryDataSet dsFirstDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.FIRST, PRECISION);
        int dsFirstLen = dsFirstDataSet.getTimestamps().length;
        List<String> dsFirstResPaths = dsFirstDataSet.getPaths();
        assertEquals(simpleLen, dsFirstResPaths.size());
        assertEquals(factSampleLen, dsFirstLen);
        assertEquals(factSampleLen, dsFirstDataSet.getValues().size());
        for (int i = 0; i < dsFirstLen; i++) {
            long dsTimestamp = dsFirstDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsFirstDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long firstNum = START_TIME + i * PRECISION;
                int pathNum = getPathNum(dsFirstResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(firstNum + pathNum, dsResult.get(j));
            }
        }
        //last
        SessionQueryDataSet dsLastDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.LAST, PRECISION);
        int dsLastLen = dsLastDataSet.getTimestamps().length;
        List<String> dsLastResPaths = dsLastDataSet.getPaths();
        assertEquals(simpleLen, dsLastResPaths.size());
        assertEquals(factSampleLen, dsLastLen);
        assertEquals(factSampleLen, dsLastDataSet.getValues().size());
        for (int i = 0; i < dsLastLen; i++) {
            long dsTimestamp = dsLastDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsLastDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long lastNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                int pathNum = getPathNum(dsLastResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(lastNum + pathNum, dsResult.get(j));
            }
        }
        //Count
        SessionQueryDataSet dsCountDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT, PRECISION);
        int dsCountLen = dsCountDataSet.getTimestamps().length;
        List<String> dsCountResPaths = dsCountDataSet.getPaths();
        assertEquals(simpleLen, dsCountResPaths.size());
        assertEquals(factSampleLen, dsCountLen);
        assertEquals(factSampleLen, dsCountDataSet.getValues().size());
        for (int i = 0; i < dsCountLen; i++) {
            long dsTimestamp = dsCountDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsCountDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long countNum = ((START_TIME + (i + 1) * PRECISION - 1) <= END_TIME) ? PRECISION : END_TIME - dsTimestamp + 1;
                assertEquals(countNum, dsResult.get(j));
            }
        }
        //Sum
        SessionQueryDataSet dsSumDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.SUM, PRECISION);
        int dsSumLen = dsSumDataSet.getTimestamps().length;
        List<String> dsSumResPaths = dsSumDataSet.getPaths();
        assertEquals(simpleLen, dsSumResPaths.size());
        assertEquals(factSampleLen, dsSumLen);
        assertEquals(factSampleLen, dsSumDataSet.getValues().size());
        for (int i = 0; i < dsSumLen; i++) {
            long dsTimestamp = dsSumDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsSumDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double sum = (dsTimestamp + maxNum) * (maxNum - dsTimestamp + 1) / 2.0;
                int pathNum = getPathNum(dsSumResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(sum + pathNum * (maxNum - dsTimestamp + 1), changeResultToDouble(dsResult.get(j)), delta);
            }
        }

        //avg
        SessionQueryDataSet dsAvgDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
        int dsAvgLen = dsAvgDataSet.getTimestamps().length;
        List<String> dsAvgResPaths = dsAvgDataSet.getPaths();
        assertEquals(simpleLen, dsAvgResPaths.size());
        assertEquals(factSampleLen, dsAvgLen);
        assertEquals(factSampleLen, dsAvgDataSet.getValues().size());
        for (int i = 0; i < dsAvgLen; i++) {
            long dsTimestamp = dsAvgDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsAvgDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsTimestamp + maxNum) / 2.0;
                int pathNum = getPathNum(dsAvgResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(avg + pathNum, changeResultToDouble(dsResult.get(j)), delta);
            }
        }*/

        //Simple delete and aggregate
        if (isAbleToDelete) {
            //deletePartialDataInColumnTest
            int removeLen = 1;
            List<String> delPartPaths = getPaths(currPath, removeLen);
            // ensure after delete there are still points in the timeseries

            //delete data
            session.deleteDataInColumns(delPartPaths, delStartTime, delEndTime);
            Thread.sleep(1000);
            SessionQueryDataSet delPartDataSet = session.queryData(paths, START_TIME, END_TIME + 1);

            int delPartLen = delPartDataSet.getTimestamps().length;
            List<String> delPartResPaths = delPartDataSet.getPaths();
            assertEquals(simpleLen, delPartResPaths.size());
            assertEquals(TIME_PERIOD, delPartDataSet.getTimestamps().length);
            assertEquals(TIME_PERIOD, delPartDataSet.getValues().size());
            for (int i = 0; i < delPartLen; i++) {
                long timestamp = delPartDataSet.getTimestamps()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = delPartDataSet.getValues().get(i);
                if (delStartTime <= timestamp && timestamp < delEndTime) {
                    for (int j = 0; j < simpleLen; j++) {
                        int pathNum = getPathNum(delPartResPaths.get(j));
                        assertNotEquals(pathNum, -1);
                        if (pathNum >= currPath + removeLen) {
                            assertEquals(timestamp + pathNum, result.get(j));
                        } else {
                            assertNull(result.get(j));
                        }
                    }
                } else {
                    for (int j = 0; j < simpleLen; j++) {
                        int pathNum = getPathNum(delPartResPaths.get(j));
                        assertNotEquals(pathNum, -1);
                        assertEquals(timestamp + pathNum, result.get(j));
                    }
                }
            }

            // Test avg for the delete
            SessionAggregateQueryDataSet delPartAvgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> delPartAvgResPaths = delPartAvgDataSet.getPaths();
            Object[] delPartAvgResult = delPartAvgDataSet.getValues();
            assertEquals(simpleLen, delPartAvgResPaths.size());
            assertEquals(simpleLen, delPartAvgDataSet.getValues().length);
            for (int i = 0; i < simpleLen; i++) {
                int pathNum = getPathNum(delPartAvgResPaths.get(i));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + removeLen) { // Here is the removed rows
                    assertEquals(deleteAvg + pathNum, changeResultToDouble(delPartAvgResult[i]), delta);
                } else {
                    assertEquals(originAvg + pathNum, changeResultToDouble(delPartAvgResult[i]), delta);
                }
            }

            // Test count for the delete
            SessionAggregateQueryDataSet delPartCountDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT);
            List<String> delPartCountResPaths = delPartCountDataSet.getPaths();
            Object[] delPartCountResult = delPartCountDataSet.getValues();
            assertEquals(simpleLen, delPartCountResPaths.size());
            assertEquals(simpleLen, delPartCountDataSet.getValues().length);
            for (int i = 0; i < simpleLen; i++) {
                int pathNum = getPathNum(delPartAvgResPaths.get(i));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + removeLen) { // Here is the removed rows
                    assertEquals(TIME_PERIOD - delTimePeriod, delPartCountResult[i]);
                } else {
                    assertEquals(TIME_PERIOD, delPartCountResult[i]);
                }
            }


            // Test downSample avg of the delete
            SessionQueryDataSet delDsAvgDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
            int delDsLen = delDsAvgDataSet.getTimestamps().length;
            List<String> delDsResPaths = delDsAvgDataSet.getPaths();
            assertEquals(factSampleLen, delDsLen);
            assertEquals(factSampleLen, delDsAvgDataSet.getValues().size());
            for (int i = 0; i < delDsLen; i++) {
                long dsStartTime = delDsAvgDataSet.getTimestamps()[i];
                assertEquals(START_TIME + i * PRECISION, dsStartTime);
                List<Object> dsResult = delDsAvgDataSet.getValues().get(i);
                for (int j = 0; j < delDsResPaths.size(); j++) {
                    long dsEndTime = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                    double delDsAvg = (dsStartTime + dsEndTime) / 2.0;
                    int pathNum = getPathNum(delDsResPaths.get(j));
                    assertNotEquals(pathNum, -1);
                    if (pathNum < currPath + removeLen) { // Here is the removed rows
                        if (dsStartTime > delEndTime || dsEndTime < delStartTime) {
                            assertEquals(delDsAvg + pathNum, changeResultToDouble(dsResult.get(j)), delta);
                        } else if (dsStartTime >= delStartTime && dsEndTime < delEndTime) {
//                            assertNull(dsResult.get(j));
                            assertTrue(Double.isNaN((Double) dsResult.get(j)));
                        } else if (dsStartTime < delStartTime) {
                            assertEquals((dsStartTime + delStartTime - 1) / 2.0 + pathNum, changeResultToDouble(dsResult.get(j)), delta);
                        } else {
                            assertEquals((dsEndTime + (delEndTime - 1) + 1) / 2.0 + pathNum, changeResultToDouble(dsResult.get(j)), delta);
                        }
                    } else {
                        assertEquals(delDsAvg + pathNum, changeResultToDouble(dsResult.get(j)), delta);
                    }
                }
            }

            currPath += simpleLen;

            //deleteAllDataInColumnTest, make new insert and delete here
            int dataInColumnLen = 3;
            List<String> delDataInColumnPaths = getPaths(currPath, dataInColumnLen);
            insertNumRecords(delDataInColumnPaths);
            //TODO add test to test if the insert is right(Is it necessary?)
            int deleteDataInColumnLen = 2;
            List<String> delAllDataInColumnPaths = getPaths(currPath, deleteDataInColumnLen);
            session.deleteDataInColumns(delAllDataInColumnPaths, START_TIME, END_TIME + 1);
            Thread.sleep(1000);
            SessionQueryDataSet delDataInColumnDataSet = session.queryData(delDataInColumnPaths, START_TIME, END_TIME + 1);
            int delDataInColumnLen = delDataInColumnDataSet.getTimestamps().length;
            List<String> delDataInColumnResPaths = delDataInColumnDataSet.getPaths();
            assertEquals(TIME_PERIOD, delDataInColumnLen);
            assertEquals(TIME_PERIOD, delDataInColumnDataSet.getValues().size());
            for (int i = 0; i < delDataInColumnLen; i++) {
                long timestamp = delDataInColumnDataSet.getTimestamps()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = delDataInColumnDataSet.getValues().get(i);
                for (int j = 0; j < dataInColumnLen; j++) {
                    int pathNum = getPathNum(delDataInColumnResPaths.get(j));
                    assertNotEquals(pathNum, -1);
                    if (pathNum < currPath + deleteDataInColumnLen) { // Here is the removed rows
                        assertNull(result.get(j));
                    } else {
                        assertEquals(timestamp + pathNum, result.get(j));
                    }
                }
            }

            /*
            // Test value filter for the delete TODO change the value filter to the right test
            int vfTime = 1123;
            String booleanExpression = COLUMN_D2_S2 + " > " + vfTime;
            SessionQueryDataSet vfDataSet = session.valueFilterQuery(delDataInColumnPaths, START_TIME, END_TIME, booleanExpression);
            int vfLen = vfDataSet.getTimestamps().length;
            List<String> vfResPaths = vfDataSet.getPaths();
            assertEquals(TIME_PERIOD + START_TIME - vfTime - 1, vfDataSet.getTimestamps().length);
            for (int i = 0; i < vfLen; i++) {
                long timestamp = vfDataSet.getTimestamps()[i];
                assertEquals(i + vfTime, timestamp);
                List<Object> result = vfDataSet.getValues().get(i);
                for (int j = 0; j < dataInColumnLen; j++) {
                    int pathNum = getPathNum(delDataInColumnResPaths.get(j));
                    assertNotEquals(pathNum, -1);
                    if (pathNum < deleteDataInColumnLen){ // Here is the removed rows
                        assertNull(result.get(j));
                    } else {
                        assertEquals(timestamp + pathNum, result.get(j));
                    }
                }
            }*/

            // Test aggregate function for the delete
            SessionAggregateQueryDataSet delDataAvgSet = session.aggregateQuery(delDataInColumnPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> delDataAvgResPaths = delDataAvgSet.getPaths();
            Object[] delDataAvgResult = delDataAvgSet.getValues();
            assertEquals(dataInColumnLen, delDataAvgResPaths.size());
            assertEquals(dataInColumnLen, delDataAvgSet.getValues().length);
            for (int i = 0; i < dataInColumnLen; i++) {
                int pathNum = getPathNum(delDataAvgResPaths.get(i));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + deleteDataInColumnLen) { // Here is the removed rows
//                    assertEquals("null", new String((byte[]) delDataAvgResult[i]));
                    assertTrue(Double.isNaN((Double) delDataAvgResult[i]));
                } else {
                    assertEquals((START_TIME + END_TIME) / 2.0 + pathNum, delDataAvgResult[i]);
                }
            }


            // Test downsample function for the delete
            SessionQueryDataSet dsDelDataInColSet = session.downsampleQuery(delDataInColumnPaths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
            int dsDelDataLen = dsDelDataInColSet.getTimestamps().length;
            List<String> dsDelDataResPaths = dsDelDataInColSet.getPaths();
            assertEquals(factSampleLen, dsDelDataLen);
            assertEquals(factSampleLen, dsDelDataInColSet.getValues().size());
            for (int i = 0; i < dsDelDataLen; i++) {
                long dsTimestamp = dsDelDataInColSet.getTimestamps()[i];
                assertEquals(START_TIME + i * PRECISION, dsTimestamp);
                List<Object> dsResult = dsDelDataInColSet.getValues().get(i);
                for (int j = 0; j < dsDelDataResPaths.size(); j++) {
                    long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                    double avg = (dsTimestamp + maxNum) / 2.0;
                    int pathNum = getPathNum(dsDelDataResPaths.get(j));
                    assertNotEquals(pathNum, -1);
                    if (pathNum < currPath + deleteDataInColumnLen) { // Here is the removed rows
//                        assertNull(dsResult.get(j));
                        assertTrue(Double.isNaN((Double) dsResult.get(j)));
                    } else {
                        assertEquals(avg + pathNum, changeResultToDouble(dsResult.get(j)), delta);
                    }
                }
            }
            currPath += dataInColumnLen;

            // deleteAllColumnsTest
            int delAllColumnLen = 3;
            List<String> delAllColumnPaths = getPaths(currPath, delAllColumnLen);
            insertNumRecords(delAllColumnPaths);
            session.deleteColumns(delAllColumnPaths);
            SessionQueryDataSet delAllColumnDataSet = session.queryData(delAllColumnPaths, START_TIME, END_TIME + 1);
            assertEquals(0, delAllColumnDataSet.getPaths().size());
            assertEquals(0, delAllColumnDataSet.getTimestamps().length);
            assertEquals(0, delAllColumnDataSet.getValues().size());
            currPath += delAllColumnLen;

            // deletePartColumnsTest
            int delPartColumnLen = 3;
            List<String> partColumnPaths = getPaths(currPath, delPartColumnLen);
            insertNumRecords(partColumnPaths);
            int delPartColumnNum = 2;
            List<String> delPartColumnPaths = getPaths(currPath, delPartColumnNum);
            session.deleteColumns(delPartColumnPaths);
            SessionQueryDataSet delPartColumnDataSet = session.queryData(partColumnPaths, START_TIME, END_TIME + 1);
            int delPartResLen = delPartColumnDataSet.getTimestamps().length;
            List<String> delPartColResPaths = delPartColumnDataSet.getPaths();
            assertEquals(delPartColumnLen - delPartColumnNum, delPartColResPaths.size());
            assertEquals(TIME_PERIOD, delPartResLen);
            assertEquals(TIME_PERIOD, delPartColumnDataSet.getValues().size());
            for (int i = 0; i < delPartResLen; i++) {
                long timestamp = delPartColumnDataSet.getTimestamps()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = delPartColumnDataSet.getValues().get(i);
                for (int j = 0; j < delPartColumnLen - delPartColumnNum; j++) {
                    int pathNum = getPathNum(delPartColResPaths.get(j));
                    assertNotEquals(pathNum, -1);
                    if (pathNum < currPath + delPartColumnNum) { // Here is the removed rows
                        fail();
                    } else {
                        assertEquals(timestamp + pathNum, result.get(j));
                    }
                }
            }
            currPath += delPartColumnLen;
        } else {
            currPath += simpleLen;
        }

        // fake data insert test
        int fakeDataLen = 2;
        List<String> fakeDataPaths = getPaths(currPath, fakeDataLen);
        insertNumRecords(fakeDataPaths);
        int count = 1;
        boolean isError = false;
        // TODO Add some more specific conditions, and try to merge this to the origin code
        try {
            insertFakeNumRecords(paths, count + TIME_PERIOD * 100);
        } catch (Exception e) {
            logger.error(e.getMessage());
            isError = true;
        } finally {
            //assertTrue(isError);
        }
        currPath += fakeDataLen;

        //dataTypeTest
        int dataTypeLen = 6;
        List<String> dataTypePaths = getPaths(currPath, dataTypeLen);
        insertDataTypeRecords(dataTypePaths, currPath);

        //queryData
        SessionQueryDataSet dtQueryDataSet = session.queryData(dataTypePaths, START_TIME, END_TIME + 1);
        int dtQueryLen = dtQueryDataSet.getTimestamps().length;
        List<String> dataTypeResPaths = dtQueryDataSet.getPaths();
        assertEquals(6, dataTypeResPaths.size());
        assertEquals(TIME_PERIOD, dtQueryLen);
        assertEquals(TIME_PERIOD, dtQueryDataSet.getValues().size());
        for (int i = 0; i < dtQueryLen; i++) {
            long timestamp = dtQueryDataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dtQueryDataSet.getValues().get(i);
            for (int j = 0; j < 6; j++) {
                int currPathPos = getPathNum(dataTypeResPaths.get(j)) - currPath;
                switch (currPathPos) {
                    case 1:
                        assertEquals((int) ((END_TIME - i) + 1), changeResultToInteger(result.get(j)));
                        break;
                    case 2:
                        assertEquals((i + 2 + START_TIME) * 1000, result.get(j));
                        break;
                    case 3:
                        assertEquals((float) (i + 3 + START_TIME + 0.01),
                            changeResultToFloat(result.get(j)), (float) delta);
                        break;
                    case 4:
                        assertEquals(((END_TIME - i) + 4 + 0.01) * 999, changeResultToDouble(result.get(j)), delta);
                        break;
                    case 5:
                        assertArrayEquals(getRandomStr(i, STRING_LEN).getBytes(), (byte[]) (result.get(j)));
                        break;
                    case 0:
                        assertEquals(i % 2 == 0, result.get(j));
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }

        //aggregateData max & avg
        List<String> dTAggrPaths = getPaths(currPath + 1, 4);
        //max
        SessionAggregateQueryDataSet dtMaxDataSet = session.aggregateQuery(dTAggrPaths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> dtMaxPaths = dtMaxDataSet.getPaths();
        Object[] dtMaxResult = dtMaxDataSet.getValues();
        assertEquals(4, dtMaxPaths.size());
        assertEquals(4, dtMaxResult.length);
        for (int i = 0; i < 4; i++) {
            int currPathPos = getPathNum(dtMaxPaths.get(i)) - currPath;
            switch (currPathPos) {
                case 1:
                    assertEquals((int) (END_TIME + 1), changeResultToInteger(dtMaxResult[i]));
                    break;
                case 2:
                    assertEquals((END_TIME + 2) * 1000, dtMaxResult[i]);
                    break;
                case 3:
                    assertEquals((float) (END_TIME + 3 + 0.01),
                        changeResultToFloat(dtMaxResult[i]), (float) delta);
                    break;
                case 4:
                    assertEquals((END_TIME + 4 + 0.01) * 999, changeResultToDouble(dtMaxResult[i]), delta);
                    break;
                default:
                    fail();
                    break;
            }
        }

        //avg
        SessionAggregateQueryDataSet dtAvgDataSet = session.aggregateQuery(dTAggrPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> dtAvgPaths = dtAvgDataSet.getPaths();
        Object[] dtAvgResult = dtAvgDataSet.getValues();
        assertEquals(4, dtAvgPaths.size());
        assertEquals(4, dtAvgResult.length);
        for (int i = 0; i < 4; i++) {
            int currPathPos = getPathNum(dtAvgPaths.get(i)) - currPath;
            switch (currPathPos) {
                case 1:
                    assertEquals((START_TIME + END_TIME) / 2.0 + 1, changeResultToDouble(dtAvgResult[i]), delta);
                    break;
                case 2:
                    assertEquals((START_TIME + END_TIME) * 500.0 + 2000, dtAvgResult[i]);
                    break;
                case 3:
                    assertEquals((START_TIME + END_TIME) / 2.0 + 3 + 0.01, changeResultToDouble(dtAvgResult[i]), delta * 10000);
                    break;
                case 4:
                    assertEquals((START_TIME + END_TIME) * 999 / 2.0 + 4.01 * 999, changeResultToDouble(dtAvgResult[i]), delta * 10000);
                    break;
                default:
                    fail();
                    break;
            }
        }

        if (isAbleToDelete) {
            //deletePartialData

            List<String> dtDelPaths = new ArrayList<>();
            dtDelPaths.add(getSinglePath(currPath, 1));
            dtDelPaths.add(getSinglePath(currPath, 3));
            dtDelPaths.add(getSinglePath(currPath, 5));

            // ensure after delete there are still points in the timeseries
            long dtDelStartTime = START_TIME + TIME_PERIOD / 5;
            long dtDelEndTime = START_TIME + TIME_PERIOD / 10 * 9;
            long dtDelTimePeriod = (dtDelEndTime - 1) - dtDelStartTime + 1;

            session.deleteDataInColumns(dtDelPaths, dtDelStartTime, dtDelEndTime);
            Thread.sleep(1000);
            SessionQueryDataSet dtDelPartDataSet = session.queryData(dataTypePaths, START_TIME, END_TIME + 1);

            int dtDelPartLen = dtDelPartDataSet.getTimestamps().length;
            List<String> dtDelPartResPaths = dtDelPartDataSet.getPaths();
            assertEquals(TIME_PERIOD, dtDelPartDataSet.getTimestamps().length);
            assertEquals(TIME_PERIOD, dtDelPartDataSet.getValues().size());
            for (int i = 0; i < dtDelPartLen; i++) {
                long timestamp = dtDelPartDataSet.getTimestamps()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = dtDelPartDataSet.getValues().get(i);
                for (int j = 0; j < 6; j++) {
                    int currPathPos = getPathNum(dtDelPartResPaths.get(j)) - currPath;
                    switch (currPathPos) {
                        case 0:
                            assertEquals(timestamp % 2 == 0, result.get(j));
                            break;
                        case 2:
                            assertEquals((timestamp + 2) * 1000, result.get(j));
                            break;
                        case 4:
                            assertEquals((4 + (END_TIME - timestamp) + START_TIME + 0.01) * 999, changeResultToDouble(result.get(j)), delta);
                            break;
                        case 1:
                            if (delStartTime <= timestamp && timestamp < delEndTime) {
                                assertNull(result.get(j));
                            } else {
                                assertEquals((int) ((END_TIME - i) + 1), changeResultToInteger(result.get(j)));
                            }
                            break;
                        case 3:
                            if (delStartTime <= timestamp && timestamp < delEndTime) {
                                assertNull(result.get(j));
                            } else {
                                assertEquals((float) (i + 3 + START_TIME + 0.01),
                                    changeResultToFloat(result.get(j)), (float) delta);
                            }
                            break;
                        case 5:
                            if (delStartTime <= timestamp && timestamp < delEndTime) {
                                assertNull(result.get(j));
                            } else {
                                assertArrayEquals(getRandomStr(i, STRING_LEN).getBytes(), (byte[]) (result.get(j)));
                            }
                            break;
                        default:
                            fail();
                            break;
                    }
                }
            }

            // Test aggregate function for the delete
            SessionAggregateQueryDataSet dtDelPartAvgDataSet = session.aggregateQuery(dTAggrPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> dtDelPartAvgResPaths = dtDelPartAvgDataSet.getPaths();
            Object[] dtDelPartAvgResult = dtDelPartAvgDataSet.getValues();
            assertEquals(4, dtDelPartAvgResPaths.size());//fixed
            assertEquals(4, dtDelPartAvgResult.length);
            for (int i = 0; i < 4; i++) {
                int currPathPos = getPathNum(dtDelPartAvgResPaths.get(i)) - currPath;
                switch (currPathPos) {
                    case 1:
                        assertEquals(((START_TIME + END_TIME) * TIME_PERIOD / 2.0 - (END_TIME -
                            (dtDelStartTime - START_TIME) + END_TIME - (dtDelEndTime - 1 - START_TIME)) * dtDelTimePeriod / 2.0)
                            / (TIME_PERIOD - dtDelTimePeriod) + 1.0, changeResultToDouble(dtDelPartAvgResult[i]), delta * 10000);
                        break;
                    case 2:
                        assertEquals((START_TIME + END_TIME) * 500.0 + 2000, dtDelPartAvgResult[i]);
                        break;
                    case 3:
                        assertEquals(((START_TIME + END_TIME) * TIME_PERIOD / 2.0 -
                            (dtDelStartTime + dtDelEndTime - 1) * dtDelTimePeriod / 2.0) / (TIME_PERIOD - dtDelTimePeriod) + 3.01, changeResultToDouble(dtDelPartAvgResult[i]), delta * 10000);
                        break;
                    case 4:
                        assertEquals((START_TIME + END_TIME) * 999 / 2.0 + 4.01 * 999, changeResultToDouble(dtDelPartAvgResult[i]), delta * 10000);
                        break;
                    default:
                        fail();
                        break;
                }
            }

            currPath += dataTypeLen;

            //deleteAll data in column, before deletion must insert first
            List<String> dataTypePaths2 = getPaths(currPath, dataTypeLen);
            insertDataTypeRecords(dataTypePaths2, currPath);
            int dtDelColumnNum = 2;
            List<String> dtDelColumnPaths = getPaths(currPath, dtDelColumnNum);
            session.deleteDataInColumns(dtDelColumnPaths, START_TIME, END_TIME + 1);
            Thread.sleep(1000);
            SessionQueryDataSet dtDelColDataSet = session.queryData(dataTypePaths2, START_TIME, END_TIME + 1);
            int dtDelColLen = dtDelColDataSet.getTimestamps().length;
            List<String> dtDelColResPaths = dtDelColDataSet.getPaths();
            assertEquals(TIME_PERIOD, dtDelColDataSet.getTimestamps().length);
            assertEquals(TIME_PERIOD, dtDelColDataSet.getValues().size());
            for (int i = 0; i < dtDelColLen; i++) {
                long timestamp = dtDelColDataSet.getTimestamps()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = dtDelColDataSet.getValues().get(i);
                for (int j = 0; j < 6; j++) {
                    int currPathPos = getPathNum(dtDelColResPaths.get(j)) - currPath;
                    if (currPathPos < dtDelColumnNum) {
                        assertNull(result.get(j));
                    } else {
                        switch (getPathNum(dtDelColResPaths.get(j)) - currPath) {
                            case 0:
                                assertEquals(timestamp % 2 == 0, result.get(j));
                                break;
                            case 1:
                                assertEquals((int) ((END_TIME - timestamp) + 1), changeResultToInteger(result.get(j)));
                                break;
                            case 2:
                                assertEquals((timestamp + 2) * 1000, result.get(j));
                                break;
                            case 3:
                                assertEquals((float) (timestamp + 3 + 0.01), changeResultToFloat(result.get(j)), (float) delta);
                                break;
                            case 4:
                                assertEquals((4 + (END_TIME - timestamp) + START_TIME + 0.01) * 999, changeResultToDouble(result.get(j)), delta);
                                break;
                            case 5:
                                assertArrayEquals(getRandomStr(i, STRING_LEN).getBytes(), (byte[]) (result.get(j)));
                                break;
                            default:
                                fail();
                                break;
                        }
                    }
                }
            }

            // Test aggregate function for the delete

            List<String> dTDeleteAggrPaths = getPaths(currPath + 1, 4);//only the 1-4 columns are numbers

            SessionAggregateQueryDataSet dtDelColAvgDataSet = session.aggregateQuery(dTDeleteAggrPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> dtDelColAvgPaths = dtDelColAvgDataSet.getPaths();
            Object[] dtDelColAvgResult = dtDelColAvgDataSet.getValues();
            assertEquals(4, dtDelColAvgPaths.size());
            assertEquals(4, dtDelColAvgDataSet.getValues().length);
            for (int i = 0; i < 4; i++) {
                int currPathPos = getPathNum(dtDelColAvgPaths.get(i)) - currPath;
                if (currPathPos < dtDelColumnNum) {
//                    assertEquals("null", new String((byte[]) dtDelColAvgResult[i]));
                    assertTrue(Double.isNaN((Double) dtDelColAvgResult[i]));
                } else {
                    switch (currPathPos) {
                        case 1:
                            assertEquals((START_TIME + END_TIME) / 2.0 + 1, changeResultToDouble(dtDelColAvgResult[i]), delta);
                            break;
                        case 2:
                            assertEquals((START_TIME + END_TIME) * 500.0 + 2000, dtDelColAvgResult[i]);
                            break;
                        case 3:
                            assertEquals((START_TIME + END_TIME) / 2.0 + 3 + 0.01, changeResultToDouble(dtDelColAvgResult[i]), delta * 10000);
                            break;
                        case 4:
                            assertEquals((START_TIME + END_TIME) * 999 / 2.0 + 4.01 * 999, changeResultToDouble(dtDelColAvgResult[i]), delta * 10000);
                            break;
                        default:
                            fail();
                            break;
                    }
                }
            }


            currPath += dataTypeLen;
        } else {
            currPath += dataTypeLen;
        }


        //multiThreadTest
/*
        //query test, multithread insert for storage; multithread query
        int mulStQueryLen = 5;
        List<String> mulStPaths = getPaths(currPath, mulStQueryLen);
        MultiThreadTask[] mulStInsertTasks = new MultiThreadTask[mulStQueryLen];
        Thread[] mulStInsertThreads = new Thread[mulStQueryLen];
        for (int i = 0; i < mulStQueryLen; i++) {
            mulStInsertTasks[i] = new MultiThreadTask(1, getPaths(currPath + i, 1), START_TIME, END_TIME,
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
        MultiThreadTask[] mulStQueryTasks = new MultiThreadTask[queryTaskNum];
        Thread[] mulStQueryThreads = new Thread[queryTaskNum];
        //each query query one storage

        for (int i = 0; i < queryTaskNum; i++) {
            mulStQueryTasks[i] = new MultiThreadTask(3, mulStPaths, START_TIME, END_TIME + 1,
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
                int len = dataSet.getTimestamps().length;
                List<String> resPaths = dataSet.getPaths();
                assertEquals(mulStQueryLen, resPaths.size());
                assertEquals(TIME_PERIOD, len);
                assertEquals(TIME_PERIOD, dataSet.getValues().size());
                for (int j = 0; j < len; j++) {
                    long timestamp = dataSet.getTimestamps()[j];
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
        MultiThreadTask[] mulTimeInsertTasks = new MultiThreadTask[mulTimeQueryLen];
        Thread[] mulTimeInsertThreads = new Thread[mulTimeQueryLen];
        for (int i = 0; i < mulTimeQueryLen; i++) {
            mulTimeInsertTasks[i] = new MultiThreadTask(1, mulTimePaths, START_TIME + i, END_TIME - (4 - i),
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
        int mulTimeResLen = mulTimeQueryDataSet.getTimestamps().length;
        List<String> mulTimeQueryResPaths = mulTimeQueryDataSet.getPaths();
        assertEquals(mulTimeQueryLen, mulTimeQueryResPaths.size());
        assertEquals(TIME_PERIOD, mulTimeResLen);
        assertEquals(TIME_PERIOD, mulTimeQueryDataSet.getValues().size());
        for (int i = 0; i < mulTimeResLen; i++) {
            long timestamp = mulTimeQueryDataSet.getTimestamps()[i];
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
            MultiThreadTask[] delPSTasks = new MultiThreadTask[delPSThreadNum];
            Thread[] delPSThreads = new Thread[delPSThreadNum];
            for (int i = 0; i < delPSThreadNum; i++) {
                delPSTasks[i] = new MultiThreadTask(2, getPaths(currPath + i, 1), delStartTime,
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
            int delPSQueryLen = delPSDataSet.getTimestamps().length;
            List<String> delPSResPaths = delPSDataSet.getPaths();
            assertEquals(mulDelPSLen, delPSResPaths.size());
            assertEquals(TIME_PERIOD, delPSQueryLen);
            assertEquals(TIME_PERIOD, delPSDataSet.getValues().size());
            for (int i = 0; i < delPSQueryLen; i++) {
                long timestamp = delPSDataSet.getTimestamps()[i];
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
            int beforePTLen = beforePTDataSet.getTimestamps().length;
            List<String> beforePTPaths = beforePTDataSet.getPaths();
            assertEquals(mulDelPTLen, beforePTPaths.size());
            assertEquals(TIME_PERIOD, beforePTLen);
            assertEquals(TIME_PERIOD, beforePTDataSet.getValues().size());
            int delPTThreadNum = 5;
            int delPTPathNum = 4;
            // the deleted paths of the data
            List<String> delPTPaths = getPaths(currPath, delPTPathNum);
            MultiThreadTask[] delPTTasks = new MultiThreadTask[delPTThreadNum];
            Thread[] delPTThreads = new Thread[delPTThreadNum];
            long delPTStartTime = START_TIME + TIME_PERIOD / 5;
            long delPTStep = TIME_PERIOD / 10;
            long delPTTimePeriod = delPTStep * delPTThreadNum;
            long delPTEndTime = delPTStartTime + TIME_PERIOD / 10 * delPTThreadNum - 1;
            for (int i = 0; i < delPTThreadNum; i++) {
                delPTTasks[i] = new MultiThreadTask(2, delPTPaths, delPTStartTime + delPTStep * i,
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
            int delPTQueryLen = delPTDataSet.getTimestamps().length;
            List<String> delPTResPaths = delPTDataSet.getPaths();
            assertEquals(mulDelPTLen, delPTResPaths.size());
            assertEquals(TIME_PERIOD, delPTQueryLen);
            assertEquals(TIME_PERIOD, delPTDataSet.getValues().size());
            for (int i = 0; i < delPTQueryLen; i++) {
                long timestamp = delPTDataSet.getTimestamps()[i];
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
            MultiThreadTask[] delASTasks = new MultiThreadTask[delASThreadNum];
            Thread[] delASThreads = new Thread[delASThreadNum];
            for (int i = 0; i < delASThreadNum; i++) {
                delASTasks[i] = new MultiThreadTask(2, getPaths(currPath + i, 1), START_TIME,
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
            int delASLen = delASDataSet.getTimestamps().length;
            List<String> delASResPaths = delASDataSet.getPaths();
            assertEquals(mulDelASLen, delASResPaths.size());
            assertEquals(TIME_PERIOD, delASLen);
            assertEquals(TIME_PERIOD, delASDataSet.getValues().size());

            for (int i = 0; i < delASLen; i++) {
                long timestamp = delASDataSet.getTimestamps()[i];
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

            MultiThreadTask[] delATTasks = new MultiThreadTask[delATThreadNum];
            Thread[] delATThreads = new Thread[delATThreadNum];

            for (int i = 0; i < delATThreadNum; i++) {
                delATTasks[i] = new MultiThreadTask(2, delATPath, delATStartTime + delATStep * i,
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
            int delATLen = delATDataSet.getTimestamps().length;
            List<String> delATResPaths = delATDataSet.getPaths();
            assertEquals(mulDelATLen, delATResPaths.size());
            assertEquals(TIME_PERIOD, delATLen);
            assertEquals(TIME_PERIOD, delATDataSet.getValues().size());
            for (int i = 0; i < delATLen; i++) {
                long timestamp = delATDataSet.getTimestamps()[i];
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
        }*/


        //addSameTypeOfStorageEngineTest
        session.addStorageEngine("127.0.0.1", defaultPort2, storageEngineType, extraParams);

        int addStorageLen = 5;
        List<String> addStoragePaths = getPaths(currPath, addStorageLen);
        insertNumRecords(addStoragePaths);
        //query Test
        SessionQueryDataSet addStQueryDataSet = session.queryData(addStoragePaths, START_TIME, END_TIME + 1);
        int addStResLen = addStQueryDataSet.getTimestamps().length;
        List<String> addStResPaths = addStQueryDataSet.getPaths();
        assertEquals(addStorageLen, addStResPaths.size());
        assertEquals(TIME_PERIOD, addStResLen);
        assertEquals(TIME_PERIOD, addStQueryDataSet.getValues().size());
        for (int i = 0; i < addStorageLen; i++) {
            long timestamp = addStQueryDataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> queryResult = addStQueryDataSet.getValues().get(i);
            for (int j = 0; j < addStorageLen; j++) {
                int pathNum = getPathNum(addStResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(timestamp + pathNum, queryResult.get(j));
            }
        }
        //aggr Count
        SessionAggregateQueryDataSet addStCountDataSet = session.aggregateQuery(addStoragePaths, START_TIME, END_TIME + 1, AggregateType.COUNT);
        assertNull(addStCountDataSet.getTimestamps());
        List<String> addStCountResPaths = addStCountDataSet.getPaths();
        Object[] addStCountResult = addStCountDataSet.getValues();
        assertEquals(addStorageLen, addStCountResPaths.size());
        assertEquals(addStorageLen, addStCountDataSet.getValues().length);
        for (int i = 0; i < addStorageLen; i++) {
            assertEquals(TIME_PERIOD, addStCountResult[i]);
        }
        //aggr Avg
        SessionAggregateQueryDataSet addStAvgDataSet = session.aggregateQuery(addStoragePaths, START_TIME, END_TIME + 1, AggregateType.AVG);
        assertNull(addStAvgDataSet.getTimestamps());
        List<String> addStAvgResPaths = addStAvgDataSet.getPaths();
        Object[] addStAvgResult = addStAvgDataSet.getValues();
        assertEquals(addStorageLen, addStAvgResPaths.size());
        assertEquals(addStorageLen, addStAvgDataSet.getValues().length);
        for (int i = 0; i < addStorageLen; i++) {
            double avg = (START_TIME + END_TIME) / 2.0;
            int pathNum = getPathNum(addStAvgResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(avg + pathNum, changeResultToDouble(addStAvgResult[i]), delta);
        }
        //deletePartial, with query, aggr count and aggr Avg
        if (isAbleToDelete) {
            int stRemoveLen = 3;
            List<String> stDelPartPaths = getPaths(currPath, stRemoveLen);
            // ensure after delete there are still points in the timeseries

            //delete data
            session.deleteDataInColumns(stDelPartPaths, delStartTime, delEndTime);
            Thread.sleep(1000);
            SessionQueryDataSet stDelPartDataSet = session.queryData(addStoragePaths, START_TIME, END_TIME + 1);
            //query
            int stDelPartLen = stDelPartDataSet.getTimestamps().length;
            List<String> stDelPartResPaths = stDelPartDataSet.getPaths();
            assertEquals(addStorageLen, stDelPartResPaths.size());
            assertEquals(TIME_PERIOD, stDelPartDataSet.getTimestamps().length);
            assertEquals(TIME_PERIOD, stDelPartDataSet.getValues().size());
            for (int i = 0; i < stDelPartLen; i++) {
                long timestamp = stDelPartDataSet.getTimestamps()[i];
                assertEquals(i + START_TIME, timestamp);
                List<Object> result = stDelPartDataSet.getValues().get(i);
                if (delStartTime <= timestamp && timestamp < delEndTime) {
                    for (int j = 0; j < addStorageLen; j++) {
                        int pathNum = getPathNum(stDelPartResPaths.get(j));
                        assertNotEquals(pathNum, -1);
                        if (pathNum >= currPath + stRemoveLen) {
                            assertEquals(timestamp + pathNum, result.get(j));
                        } else {
                            assertNull(result.get(j));
                        }
                    }
                } else {
                    for (int j = 0; j < addStorageLen; j++) {
                        int pathNum = getPathNum(stDelPartResPaths.get(j));
                        assertNotEquals(pathNum, -1);
                        assertEquals(timestamp + pathNum, result.get(j));
                    }
                }
            }

            // Test avg for the delete
            SessionAggregateQueryDataSet stDelPartAvgDataSet = session.aggregateQuery(addStoragePaths, START_TIME, END_TIME + 1, AggregateType.AVG);
            List<String> stDelPartAvgResPaths = stDelPartAvgDataSet.getPaths();
            Object[] stDelPartAvgResult = stDelPartAvgDataSet.getValues();
            assertEquals(addStorageLen, stDelPartAvgResPaths.size());
            assertEquals(addStorageLen, stDelPartAvgDataSet.getValues().length);
            for (int i = 0; i < addStorageLen; i++) {
                int pathNum = getPathNum(stDelPartAvgResPaths.get(i));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + stRemoveLen) { // Here is the removed rows
                    assertEquals(deleteAvg + pathNum, changeResultToDouble(stDelPartAvgResult[i]), delta);
                } else {
                    assertEquals(originAvg + pathNum, changeResultToDouble(stDelPartAvgResult[i]), delta);
                }
            }

            // Test count for the delete
            SessionAggregateQueryDataSet stDelPartCountDataSet = session.aggregateQuery(addStoragePaths, START_TIME, END_TIME + 1, AggregateType.COUNT);
            List<String> stDelPartCountResPaths = stDelPartCountDataSet.getPaths();
            Object[] stDelPartCountResult = stDelPartCountDataSet.getValues();
            assertEquals(addStorageLen, stDelPartCountResPaths.size());
            assertEquals(addStorageLen, stDelPartCountDataSet.getValues().length);
            for (int i = 0; i < addStorageLen; i++) {
                int pathNum = getPathNum(stDelPartAvgResPaths.get(i));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + stRemoveLen) { // Here is the removed rows
                    assertEquals(TIME_PERIOD - delTimePeriod, stDelPartCountResult[i]);
                } else {
                    assertEquals(TIME_PERIOD, stDelPartCountResult[i]);
                }
            }
        }
        logger.info("session test finished");
    }

    private class MultiThreadTask implements Runnable {

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
