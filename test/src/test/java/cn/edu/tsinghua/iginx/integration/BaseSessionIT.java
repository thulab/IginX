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
import cn.edu.tsinghua.iginx.integration.insert.BaseDataProcessType;
import cn.edu.tsinghua.iginx.integration.insert.DataTypeInsert;
import cn.edu.tsinghua.iginx.integration.insert.MultiThreadInsert;
import cn.edu.tsinghua.iginx.integration.insert.OrderInsert;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;


public abstract class BaseSessionIT {

    protected static final Logger logger = LoggerFactory.getLogger(BaseSessionIT.class);
    protected static final long TIME_PERIOD = 10000L;
    protected static final long START_TIME = 1000L;
    protected static final long END_TIME = START_TIME + TIME_PERIOD - 1;
    protected static final double delta = 1e-3;

    protected boolean isFromZero;//DownSample param, influxDB use 0 to divide windows, but IoTDB use startTime

    protected static Session session;
    protected int currPath = 0;
    protected boolean isAbleToDelete;
    protected String storageEngineType;
    protected int defaultPort2;
    protected Map<String, String> extraParams;

    protected int insertTypeNum;
    protected int deleteTypeNum;
    protected BaseDataProcessType[] insertTypeList;
    protected int totalInsertNum;
    protected int basicStart, basicEnd;
    protected int dataTypeStart, dataTypeEnd;
    protected int multiThreadStart, multiThreadEnd;
    protected int errorOperationStart, errorOperationEnd;
    protected int addStorageStart, addStorageEnd;

    @Before
    public void setUp() {
        try {
            session = new Session("127.0.0.1", 6888, "root", "root");
            session.openSession();
            initializeInsertType();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @After
    public void tearDown() throws SessionException {
        session.closeSession();
    }

    @Test
    public void newSessionTest() {
        basicTest();
        dataTypeTest();
        multiThreadTest();
        addStorageTest();
        errorOperationTest();
    }

    private void basicTest() {
        try {
            insertData(basicStart, basicEnd);
            queryTest(basicStart, basicEnd);
            aggregateTest(basicStart, basicEnd);
            //downSampleTest(basicStart, basicEnd);
            if (isAbleToDelete) {
                deleteData(basicStart, basicEnd);
                Thread.sleep(1000);
                queryTest(basicStart, basicEnd);
                aggregateTest(basicStart, basicEnd);
                //downSampleTest(basicStart, basicEnd);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error occurs in basic Test");
            fail();
        }
        logger.info("Basic test finished!");
    }

    private void dataTypeTest(){
        try {
            insertData(dataTypeStart, dataTypeEnd);
            queryTest(dataTypeStart, dataTypeEnd);
            aggregateTest(dataTypeStart, dataTypeEnd);
            //downSampleTest(dataTypeStart, dataTypeEnd);
            if (isAbleToDelete) {
                deleteData(dataTypeStart, dataTypeEnd);
                Thread.sleep(1000);
                queryTest(dataTypeStart, dataTypeEnd);
                aggregateTest(dataTypeStart, dataTypeEnd);
                //downSampleTest(dataTypeStart, dataTypeEnd);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error occurs in dataType Test");
            fail();
        }
        session.insertColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
        logger.info("DataType test finished!");
    }

    private void multiThreadTest(){
        try {
            insertData(multiThreadStart, multiThreadEnd);
            queryTest(multiThreadStart, multiThreadEnd);
            aggregateTest(multiThreadStart, multiThreadEnd);
            multiThreadQueryTest(multiThreadStart, multiThreadEnd);
            multiAggrQueryTest(multiThreadStart, multiThreadEnd);
            if (isAbleToDelete) {
                deleteData(multiThreadStart, multiThreadEnd);
                Thread.sleep(1000);
                queryTest(multiThreadStart, multiThreadEnd);
                aggregateTest(multiThreadStart, multiThreadEnd);
                multiThreadQueryTest(multiThreadStart, multiThreadEnd);
                multiAggrQueryTest(multiThreadStart, multiThreadEnd);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error occurs in multiThread Test");
            fail();
        }
        logger.info("MultiThread test finished!");
    }

    // TODO how to ensure is the data inserted into the right storage?
    protected void addStorageTest(){
        try {
            addStorage();
            insertData(addStorageStart, addStorageEnd);
            queryTest(addStorageStart, addStorageEnd);
            aggregateTest(addStorageStart, addStorageEnd);
            //downSampleTest(basicStart, basicEnd);
            if (isAbleToDelete) {
                deleteData(addStorageStart, addStorageEnd);
                Thread.sleep(1000);
                queryTest(addStorageStart, addStorageEnd);
                aggregateTest(addStorageStart, addStorageEnd);
                //downSampleTest(basicStart, basicEnd);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error occurs in add storage Test");
            fail();
        }
        logger.info("Add storage test finished!");
    }

    //TODO depend on database
    protected abstract void errorOperationTest();

    private void initializeInsertType(){
        insertTypeNum = 4;
        deleteTypeNum = 4;
        totalInsertNum = insertTypeNum * deleteTypeNum;
        insertTypeList = new BaseDataProcessType[totalInsertNum];
        basicStart = 0;
        basicEnd = deleteTypeNum;
        dataTypeStart = deleteTypeNum;
        dataTypeEnd = deleteTypeNum * 2;
        multiThreadStart = deleteTypeNum * 2;
        multiThreadEnd = deleteTypeNum * 3;
        addStorageStart = deleteTypeNum * 3;
        addStorageEnd = deleteTypeNum * 4;
        int currPos = 0;
        for(int i = 0; i < insertTypeNum; i++){
            for(int j = 0; j < deleteTypeNum; j++) {
                //TODO how to set the pathlength of one insert??????
                int pathLen = 3;//TODO make them as a constant?
                insertTypeList[currPos] = getInsertType(i, j, pathLen);
                currPos++;
            }
        }
    }

    private BaseDataProcessType getInsertType(int insertType, int deleteType, int pathLength){
        boolean isColumnInsert = (Math.random() > 0.5);
        BaseDataProcessType ins;
        switch (insertType){
            case 0:
            case 3:
                ins = new OrderInsert(session, (int)START_TIME, (int)END_TIME, isColumnInsert,
                        deleteType, currPath, pathLength);
                currPath = currPath + ins.getPathLength();
                return ins;
            case 1:
                ins = new DataTypeInsert(session, (int)START_TIME, (int)END_TIME, true,
                        deleteType, currPath);
                currPath = currPath + ins.getPathLength();
                return ins;
            case 2:
                ins = new MultiThreadInsert(session, (int)START_TIME, (int)END_TIME, true,
                        deleteType, currPath, pathLength * 3, (Math.random() > 0.5), (Math.random() > 0.5), 3);
                currPath = currPath + ins.getPathLength();
                return ins;
            default:
                return null;
        }
    }

    protected int[] getQueryTime(int startTime, int endTime, int queryType){
        int[] result = new int[2];
        switch(queryType){
            case 0: // full input and output
                result[0] = startTime;
                result[1] = endTime;
                break;
            case 1: // larger range
                result[0] = startTime / 2;
                result[1] = endTime * 2;
                break;
            case 2: // half range of the insert
                result[0] = (startTime + endTime) / 2;
                result[1] = endTime + (startTime + endTime) / 2;
                break;
            case 3: // inside the insert
                result[0] = (startTime + endTime) / 2;
                result[1] = (startTime + endTime) * 3 / 4;
                break;
            case 4: // no result in the insert
                result[0] = endTime * 2 + 1;
                result[1] = endTime * 3 + 1;
                break;
            case 5: // border
                result[0] = endTime;
                result[1] = endTime + 3;
                break;
        }
        return result;
    }

    private void queryTest(int queryStart, int queryEnd) throws ExecutionException, SessionException {
        for(int i = queryStart; i < queryEnd; i++){
            BaseDataProcessType insert = insertTypeList[i];
            for(int j = 0; j < 6; j++) {
                int[] queryTime = getQueryTime((int)START_TIME, (int)END_TIME, j);
                SessionQueryDataSet simpleQueryDataSet = session.queryData(insert.getInsertPaths(),
                        queryTime[0], queryTime[1] + 1);
                verifyQueryResult(simpleQueryDataSet, insert, queryTime[0], queryTime[1],
                        insert.getInsertPaths());
            }
        }
    }

    private void multiThreadQueryTest(int queryStart, int queryEnd) {
        for(int i = queryStart; i < queryEnd; i++){
            MultiThreadInsert insert = (MultiThreadInsert)insertTypeList[i];
            int threadNum = insert.getThreadNum();
            for(int queryTypeNum = 0; queryTypeNum < 6; queryTypeNum++) {
                for(int k = 0; k < 2; k++) {
                    int[] queryTime = getQueryTime((int) START_TIME, (int) END_TIME, queryTypeNum);
                    int startTime = queryTime[0];
                    int endTime = queryTime[1];
                    List<String> insertPaths = insert.getInsertPaths();
                    List<List<String>> paths = new LinkedList<>();
                    List<Integer> startTimes = new LinkedList<>();
                    List<Integer> endTimes = new LinkedList<>();
                    List<AggregateType> aggrTypes = new LinkedList<>();
                    if(k == 0){
                        // divide different threads by time
                        int timePeriod = (endTime - startTime + 1) / threadNum;
                        for(int l = 0; l < threadNum; l++){
                            paths.add(insertPaths);
                            startTimes.add(startTime + l * timePeriod);
                            endTimes.add(l == threadNum - 1 ? endTime : startTime + (l + 1) * timePeriod - 1);
                            aggrTypes.add(null);
                        }
                    } else {
                        // divide different threads by pathNum
                        int pathNumInThread = insertPaths.size() / threadNum;
                        for(int l = 0; l < threadNum; l++){
                            List<String> threadPaths = new LinkedList<>();
                            int pathEnd = (l == threadNum - 1 ? insertPaths.size() : (l + 1) * pathNumInThread);
                            for(int m = l * pathNumInThread; m < pathEnd; m++) {
                                threadPaths.add(insertPaths.get(m));
                            }
                            paths.add(threadPaths);
                            startTimes.add(startTime);
                            endTimes.add(endTime);
                            aggrTypes.add(null);
                        }
                    }

                    List<Object> aggrDataSets =
                            insert.multiThreadQuery(paths, startTimes, endTimes, aggrTypes);

                    for (int thread = 0; thread < insert.getThreadNum(); thread++) {
                        verifyQueryResult((SessionQueryDataSet) aggrDataSets.get(thread),
                                insert, startTimes.get(thread), endTimes.get(thread), paths.get(thread));
                    }
                }
            }
        }
    }

    private void aggregateTest(int queryStart, int queryEnd) throws ExecutionException, SessionException {
        int insertTypeNum = 0, queryTypeNum = 0, aggrTypeNum = 0, pathNum = 0;
        try {
            for (insertTypeNum = queryStart; insertTypeNum < queryEnd; insertTypeNum++) {
                BaseDataProcessType insert = insertTypeList[insertTypeNum];
                for (queryTypeNum = 0; queryTypeNum < 6; queryTypeNum++) {
                    int[] queryTime = getQueryTime((int) START_TIME, (int) END_TIME, queryTypeNum);
                    for (aggrTypeNum = 0; aggrTypeNum < 7; aggrTypeNum++) {
                        AggregateType aggrType = AggregateType.findByValue(aggrTypeNum);
                        SessionAggregateQueryDataSet aggrQueryDataSet = session.aggregateQuery(insert.getValidAggrPaths(),
                                queryTime[0], queryTime[1] + 1, aggrType);
                        verifyAggrQueryResult(aggrQueryDataSet, insert, queryTime[0], queryTime[1], aggrType, insert.getValidAggrPaths());
                    }
                }
            }
        } catch (Exception e){
            logger.error("Error occurs in AggregateTest, where insertType = "+ insertTypeNum + ", queryType = "
                    + queryTypeNum + ", aggrtype = " + AggregateType.findByValue(aggrTypeNum).toString()
                    + ", pathlist = " + getSinglePath(pathNum, 0));
            throw e;
        }
    }

    private void multiAggrQueryTest(int queryStart, int queryEnd) {
        int insertTypeNum = 0, queryTypeNum = 0, aggrTypeNum = 0, pathNum = 0;
        try {
            for (insertTypeNum = queryStart; insertTypeNum < queryEnd; insertTypeNum++) {
                MultiThreadInsert insert = (MultiThreadInsert) insertTypeList[insertTypeNum];
                int threadNum = insert.getThreadNum();
                for (queryTypeNum = 0; queryTypeNum < 6; queryTypeNum++) {
                    for (aggrTypeNum = 0; aggrTypeNum < 7; aggrTypeNum++) {
                        for(int i = 0; i < 2; i++) {
                            int[] queryTime = getQueryTime((int) START_TIME, (int) END_TIME, queryTypeNum);
                            int startTime = queryTime[0];
                            int endTime = queryTime[1];
                            AggregateType aggrType = AggregateType.findByValue(aggrTypeNum);
                            List<String> validPaths = insert.getValidAggrPaths();
                            List<List<String>> paths = new LinkedList<>();
                            List<Integer> startTimes = new LinkedList<>();
                            List<Integer> endTimes = new LinkedList<>();
                            List<AggregateType> aggrTypes = new LinkedList<>();
                            if (i == 0) {
                                // divide different threads by time
                                int timePeriod = (endTime - startTime + 1) / threadNum;
                                for (int l = 0; l < threadNum; l++) {
                                    paths.add(validPaths);
                                    startTimes.add(startTime + l * timePeriod);
                                    endTimes.add(l == threadNum - 1 ? endTime : startTime + (l + 1) * timePeriod - 1);
                                    aggrTypes.add(AggregateType.findByValue(aggrTypeNum));
                                }
                            } else {
                                // divide different threads by pathNum
                                int pathNumInThread = validPaths.size() / threadNum;
                                for (int l = 0; l < threadNum; l++) {
                                    List<String> threadPaths = new LinkedList<>();
                                    int pathEnd = (l == threadNum - 1 ? validPaths.size() : (l + 1) * pathNumInThread);
                                    for (int m = l * pathNumInThread; m < pathEnd; m++) {
                                        threadPaths.add(validPaths.get(m));
                                    }
                                    paths.add(threadPaths);
                                    startTimes.add(startTime);
                                    endTimes.add(endTime);
                                    aggrTypes.add(AggregateType.findByValue(aggrTypeNum));
                                }
                            }
                            List<Object> aggrDataSets =
                                    insert.multiThreadQuery(paths, startTimes, endTimes, aggrTypes);

                            for (int thread = 0; thread < insert.getThreadNum(); thread++) {
                                verifyAggrQueryResult((SessionAggregateQueryDataSet) aggrDataSets.get(thread),
                                        insert, startTimes.get(thread), endTimes.get(thread), aggrType, paths.get(thread));
                            }
                        }
                    }
                }
            }
        } catch (Exception e){
            logger.error("Error occurs in MultiThreadAggregateTest, where insertType = "+ insertTypeNum + ", queryType = "
                    + queryTypeNum + ", aggrtype = " + AggregateType.findByValue(aggrTypeNum).toString()
                    + ", pathlist = " + getSinglePath(pathNum, 0));
            throw e;
        }
    }

    private void verifyQueryResult(SessionQueryDataSet dataSet, BaseDataProcessType insert, int queryStart, int queryEnd,
                                   List<String> queryPaths){
        int simpleResLen = dataSet.getTimestamps().length;
        List<String> queryResPaths = dataSet.getPaths();
        //assertEquals(queryPaths.size(), queryResPaths.size()); TODO 目前只能保证出现的每个结果都是正确的
        int currTimeStamp = queryStart;
        int finalTimeStamp = queryEnd;
        for (int k = 0; k < simpleResLen; k++) {
            long timeStamp = dataSet.getTimestamps()[k];
            while(currTimeStamp < timeStamp){
                for(int l = 0; l < queryResPaths.size(); l++) {
                    int pathNum = getPathNum(queryResPaths.get(l));
                    assertNull(insert.getQueryResult(currTimeStamp, pathNum));
                }
                currTimeStamp++;
            }
            List<Object> queryResult = dataSet.getValues().get(k);
            for (int l = 0; l < queryResPaths.size(); l++) {
                int pathNum = getPathNum(queryResPaths.get(l));
                assertNotEquals(pathNum, -1);
                DataType type = insert.getDataTypeList().get(pathNum - insert.getPathStart());
                Object aimResult = insert.getQueryResult(currTimeStamp, pathNum);
                if (aimResult == null) {
                    assertNull(queryResult.get(l));
                } else {
                    switch (type) {
                        case BOOLEAN:
                            assertEquals(aimResult, queryResult.get(l));
                            break;
                        case LONG:
                            assertEquals((long)aimResult, changeResultToLong(queryResult.get(l)));
                            break;
                        case INTEGER:
                            assertEquals((int)aimResult, changeResultToInteger(queryResult.get(l)));
                            break;
                        case FLOAT:
                            assertEquals((float)aimResult, changeResultToFloat(queryResult.get(l)), delta);
                            break;
                        case DOUBLE:
                            assertEquals((double)aimResult, changeResultToDouble(queryResult.get(l)), delta);
                            break;
                        case BINARY:
                            assertArrayEquals((byte[])aimResult, (byte[])(queryResult.get(l)));
                            break;
                    }
                }
            }
            currTimeStamp++;
        }
        // Ensure there are no points after the query result
        while(currTimeStamp <= finalTimeStamp){
            for(int l = 0; l < queryResPaths.size(); l++) {
                int pathNum = getPathNum(queryResPaths.get(l));
                assertNull(insert.getQueryResult(currTimeStamp, pathNum));
            }
            currTimeStamp++;
        }
    }

    private void verifyAggrQueryResult(SessionAggregateQueryDataSet dataSet, BaseDataProcessType insert, int queryStart,
                                       int queryEnd, AggregateType aggrType, List<String> queryPaths){
        List<String> queryResPaths = dataSet.getPaths();
        int resPathLen = queryResPaths.size();
        //TODO 缺少一个用于检查结果个数是否正确的部分
        //assertEquals(resPathLen, queryPaths.size());
        Object[] queryResult = dataSet.getValues();
        for (int l = 0; l < resPathLen; l++) {
            int pathNum = getPathNum(queryResPaths.get(l));
            assertNotEquals(pathNum, -1);
            Object res = getNullResult(insert.getAggrQueryResult(aggrType, queryStart, queryEnd, pathNum));
            if (res == null) {
                assertNull(getNullResult(queryResult[l]));
            } else {
                switch (aggrType) {
                    case MAX:
                    case MIN:
                    case FIRST_VALUE:
                    case LAST_VALUE:
                    case AVG:
                        assertEquals(changeResultToDouble(res), changeResultToDouble(queryResult[l]), delta);
                        break;
                    case SUM:
                        //TODO queryTime不是实际的区间的时间。是否可能会导致更大的误差未被检测出来？
                        assertEquals(changeResultToDouble(res), changeResultToDouble(queryResult[l]),
                                delta * (queryEnd - queryStart));
                        break;
                }
            }
        }
    }

    // TODO the main branch still not fix this problem
    protected void downSampleTest() {
        int insertTypeNum = 0, queryTypeNum = 0, aggrTypeNum = 0, pathNum = 0, timePeriod = 0;
        try {
            for (insertTypeNum = 0; insertTypeNum < totalInsertNum; insertTypeNum++) {
                BaseDataProcessType insert = insertTypeList[insertTypeNum];
                for (queryTypeNum = 0; queryTypeNum < 6; queryTypeNum++) {
                    int[] queryTime = getQueryTime((int) START_TIME, (int) END_TIME, queryTypeNum);
                    int[] downSamplePeriod = getQueryPeriod(queryTime[0], queryTime[1], isFromZero);
                    for (aggrTypeNum = 0; aggrTypeNum < 7; aggrTypeNum++) {
                        for (timePeriod = 0; timePeriod < 4; timePeriod++) {
                            AggregateType aggrType = AggregateType.findByValue(aggrTypeNum);
                            int period = downSamplePeriod[timePeriod];
                            SessionQueryDataSet dsAggrQueryDataSet = session.downsampleQuery(insert.getInsertPaths(),
                                    queryTime[0], queryTime[1] + 1, aggrType, (long)period);
                            List<String> queryResPaths = dsAggrQueryDataSet.getPaths();
                            int resPathLen = queryResPaths.size();
                            List<List<Object>> queryResult = dsAggrQueryDataSet.getValues();
                            for (int l = 0; l < resPathLen; l++) {
                                pathNum = getPathNum(queryResPaths.get(l));
                                assertNotEquals(pathNum, -1);
                                Object[] aimResult = insert.getDownSampleQueryResult(aggrType, queryTime[0], queryTime[1], pathNum, period, isFromZero);
                                assertEquals(aimResult.length, queryResult.size());
                                for(int i = 0; i < aimResult.length; i++) {
                                    Object result = getNullResult(aimResult[i]);
                                    if (result == null) {
                                        assertNull(getNullResult(queryResult.get(i).get(l)));
                                    } else {
                                        assertEquals(changeResultToDouble(aimResult[i]),
                                                changeResultToDouble(queryResult.get(i).get(l)), delta);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e){
            logger.error("Error occurs in DownSampleTest, where insertType = "+ insertTypeNum + ", queryType = "
                    + queryTypeNum + ", aggrtype = " + AggregateType.findByValue(aggrTypeNum).toString()
                    + ", pathlist = " + getSinglePath(pathNum, 0) + ", timePeriod = "+ timePeriod);
        }
    }

    protected abstract void addStorage();

    private void insertData(int startNum, int endNum){
        for(int i = startNum; i < endNum; i++) {
            BaseDataProcessType insert = insertTypeList[i];
            insert.insert();
        }
    }

    private void deleteData(int startNum, int endNum){
        for(int i = startNum; i < endNum; i++) {
            BaseDataProcessType insert = insertTypeList[i];
            insert.delete();
        }
    }

    protected String getSinglePath(int startPosition, int offset) {
        int pos = startPosition + offset;
        return ("sg1.d" + pos + ".s" + pos);
    }

    protected int getPathNum(String path) {
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

    protected void insertFakeNumRecords(List<String> insertPaths, long count) throws SessionException, ExecutionException {
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
                if(i == 0) {
                    values[(int) j] = pathNum + j + START_TIME + 0.0001;
                } else {
                    values[(int) j] = pathNum + j + START_TIME;
                }
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new LinkedList<>();
        for (int i = 0; i < pathLen; i++) {
            if(i == 0) {
                dataTypeList.add(DataType.DOUBLE);
            } else {
                dataTypeList.add(DataType.LONG);
            }
        }
        session.insertColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
    }

    protected Object getNullResult(Object rawResult) {
        if (rawResult instanceof byte[]) {
            String resultStr = new String((byte[]) rawResult);
            if (resultStr.equals("null")) {
                return null;
            } else {
                System.out.println("error1");
            }
        }
        return rawResult;
    }

    protected long changeResultToLong(Object rawResult) {
        long result = 0;
        if (rawResult instanceof java.lang.Long){
            return (long)rawResult;
        } else if (rawResult instanceof java.lang.Integer) {
            result = (int)rawResult;
        } else {
            try {
                result = (long)rawResult;
            } catch (Exception e) {
                logger.error(e.getMessage());
                fail();
            }
        }
        return result;
    }

    protected int changeResultToInteger(Object rawResult) {
        int result = 0;
        if (rawResult instanceof java.lang.Integer){
            return (int)rawResult;
        } else if (rawResult instanceof java.lang.Long) {
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

    protected double changeResultToDouble(Object rawResult) {
        //System.out.println(rawResult);
        double result = 0;
        if (rawResult instanceof java.lang.Double){
            return (double)rawResult;
        } else if(rawResult instanceof java.lang.Float) {
            result = (float)(rawResult);
        } else if (rawResult instanceof java.lang.Long){
            result = (double)((long)rawResult);
        } else if(rawResult instanceof java.lang.Integer){
            result = (int)rawResult;
        } else if(rawResult instanceof byte[]) {
            String resultStr = new String((byte[]) rawResult);
            //System.out.println(resultStr);
            result = Double.parseDouble(resultStr);
        } else {
            try {
                result = (double)rawResult;
            } catch (Exception e){
                e.printStackTrace();
                System.out.println("Error in change result in double, rawResult is + " + rawResult);
                fail();
            }
        }
        return result;
    }

    protected float changeResultToFloat(Object rawResult){
        float result = 0;
        if (rawResult instanceof java.lang.Float){
            return (float)rawResult;
        } else if (rawResult instanceof java.lang.Double){
            result = (float)((double)rawResult);
        } else {
            try {
                result = (float)rawResult;
            } catch (Exception e){
                logger.error(e.getMessage());
                fail();
            }
        }
        return result;
    }

    protected int[] getQueryPeriod(int startTime, int endTime, boolean isFromZero){
        int[] period = new int[4];
        if(!isFromZero){
            period[0] = 5;
            period[1] = (endTime - startTime) / 10;
            if(period[1] < 2){
                period[1] = 2;
            }
            period[2] = (endTime - startTime) / 97;
            if(period[2] < 3){
                period[2] = 3;
            }
            period[3] = (endTime - startTime) / 997;
            if(period[3] < 5){
                period[3] = 5;
            }
        } else{
            period[0] = 3;
            period[1] = 10;
            period[2] = 97;
            period[3] = 123;
        }
        return period;
    }
}
