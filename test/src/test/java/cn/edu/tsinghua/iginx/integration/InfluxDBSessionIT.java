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
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class InfluxDBSessionIT {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBSessionIT.class);
    private static final String S1 = "sg.d1.s5";
    private static final String S2 = "sg.d2.s6";
    private static final String S3 = "sg.d3.s7";
    private static final String S4 = "sg.d4.s8";
    private static final long COLUMN_START_TIMESTAMP = 0L;
    private static final long COLUMN_END_TIMESTAMP = 10500L;
    private static final long COLUMN_SIZE = COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP;
    private static final long ROW_START_TIMESTAMP = 10501L;
    private static final long ROW_END_TIMESTAMP = 21000L;
    private static final int ROW_INTERVAL = 10;
    private static final long ROW_SIZE = (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP) / 10 + 1;
    private static Session session;
    private List<String> paths = new ArrayList<>();

    @Before
    public void setUp() {
        try {
            paths.add(S1);
            paths.add(S2);
            paths.add(S3);
            paths.add(S4);
            session = new Session("127.0.0.1", 6888, "root", "root");
            session.openSession();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @After
    public void tearDown() throws SessionException {
        session.closeSession();
    }

    @Test
    public void InfluxDBTest() throws Exception {

        // 列式插入数据
        insertColumnRecords();

        // 行式插入数据
        insertRowRecords();

        // 查询数据
        long startTime = COLUMN_START_TIMESTAMP;
        long endTime = ROW_END_TIMESTAMP;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);

        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(4, resPaths.size());
        assertEquals(COLUMN_SIZE + ROW_SIZE, len);
        assertEquals(COLUMN_SIZE + ROW_SIZE, dataSet.getValues().size());
        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            if (i < COLUMN_SIZE) {
                assertEquals(i, timestamp);
                List<Object> result = dataSet.getValues().get(i);
                for (int j = 0; j < 4; j++) {
                    assertEquals(timestamp + getPathNum(resPaths.get(j)), (long)result.get(j));
                }
            } else {
                assertEquals(ROW_START_TIMESTAMP + (i - COLUMN_SIZE) * ROW_INTERVAL, timestamp);
                List<Object> result = dataSet.getValues().get(i);
                for (int j = 0; j < 4; j++) {
                    assertEquals(timestamp + getPathNum(resPaths.get(j)), (long)result.get(j));
                }
            }
        }

        /*
        // 值过滤查询
        valueFilterQuery();

        // 聚合查询数据
        aggregateQuery();

        // 降采样聚合查询
        downsampleQuery();*/

    }

    private static void insertColumnRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP);
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i + COLUMN_START_TIMESTAMP;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                values[(int) j] = i + j;
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            dataTypeList.add(DataType.LONG);
        }

        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        //TODO there is a problem here
        int size = (int) (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP) / ROW_INTERVAL + 1;
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            long timestamp = ROW_START_TIMESTAMP + i * ROW_INTERVAL;
            timestamps[(int) i] = timestamp;
            Object[] values = new Object[4];
            for (long j = 0; j < 4; j++) {
                values[(int) j] = timestamp + j;
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            dataTypeList.add(DataType.LONG);
        }

        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }


    private static int getPathNum(String sg){
        switch (sg){
            case S1:
                return 0;
            case S2:
                return 1;
            case S3:
                return 2;
            case S4:
                return 3;
            default:
                return -1;
        }
    }

    /*
    private static List<String> getSinglePathList(int num){
        List<String> path = new ArrayList<>();
        switch(num){
            case 0:
                path.add(S1);
                break;
            case 1:
                path.add(S2);
                break;
            case 2:
                path.add(S3);
                break;
            case 3:
                path.add(S4);
                break;
            default:
                break;
        }
        return path;
    }*/


    private static void aggregateQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;

        // MAX
        SessionAggregateQueryDataSet dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MAX);
        dataSet.print();

        // MIN
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MIN);
        dataSet.print();

        // FIRST
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST);
        dataSet.print();

        // LAST
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.LAST);
        dataSet.print();

        // COUNT
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.COUNT);
        dataSet.print();

        // SUM
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.SUM);
        dataSet.print();

        // AVG
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.AVG);
        dataSet.print();
    }

    private static void downsampleQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        long startTime = ROW_START_TIMESTAMP;
        long endTime = ROW_END_TIMESTAMP + 1;

        System.out.println("Downsample Query: ");

        // MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, ROW_INTERVAL * 100);
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, ROW_INTERVAL * 100);
        dataSet.print();

        // FIRST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST, ROW_INTERVAL * 100);
        dataSet.print();

        // LAST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST, ROW_INTERVAL * 100);
        dataSet.print();

        // COUNT
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.COUNT, ROW_INTERVAL * 100);
        dataSet.print();

        // SUM
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.SUM, ROW_INTERVAL * 100);
        dataSet.print();

        // AVG
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.AVG, ROW_INTERVAL * 100);
        dataSet.print();

        // 降采样查询结束
        System.out.println("Downsample Query Finished.");
    }

    private static void deleteDataInColumns() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S3);
        paths.add(S4);

        long startTime = COLUMN_END_TIMESTAMP - 50L;
        long endTime = ROW_START_TIMESTAMP + 50L;

        session.deleteDataInColumns(paths, startTime, endTime);
    }

    private static void valueFilterQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;
        String booleanExpression = S2 + " > 3";
        SessionQueryDataSet dataSet = session.valueFilterQuery(paths, startTime, endTime, booleanExpression);
        dataSet.print();
    }
}
