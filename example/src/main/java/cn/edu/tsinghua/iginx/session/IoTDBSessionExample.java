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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class IoTDBSessionExample {

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
    private static final int INTERVAL = 10;
    private static Session session;

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();


        for (int i = 0; i < 100; i += 4) {
            int biggerPathNum = 20000;
            System.out.println("Insert path num: " + biggerPathNum);
            testInsertNonAlignedRowRecords(System.currentTimeMillis() + i, biggerPathNum);
            testInsertNonAlignedColumnRecords(System.currentTimeMillis() + i + 1, biggerPathNum);

            System.out.println();

            int smallerPathNum = 1200;
            System.out.println("Insert path num: " + smallerPathNum);
            testInsertRowRecords(System.currentTimeMillis() + i + 2, smallerPathNum);
            testInsertColumnRecords(System.currentTimeMillis() + i + 3, smallerPathNum);
        }


//        // 列式插入对齐数据
//        insertColumnRecords();
//        // 列式插入非对齐数据
//        insertNonAlignedColumnRecords();
//        // 行式插入对齐数据
//        insertRowRecords();
//        // 行式插入非对齐数据
//        insertNonAlignedRowRecords();
//        // 查询数据
//        queryData();
//        // 值过滤查询
//        valueFilterQuery();
//        // 聚合查询
//        aggregateQuery();
//        // Last 查询
//        lastQuery();
//        // 降采样聚合查询
//        downsampleQuery();
//        // 删除数据
//        deleteDataInColumns();
//        // 再次查询数据
//        queryData();

        // 关闭 Session
        session.closeSession();
    }

    private static void testInsertNonAlignedRowRecords(long time, int pathNum) throws ExecutionException, SessionException {
        List<String> path = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            path.add("1001.SUCC" + i);
        }
        long[] timestamps = new long[1];
        timestamps[0] = time;
        Object[] values = new Object[1];
        List<Object> valueList = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            valueList.add(323.0);
        }
        values[0] = valueList.toArray();
        List<DataType> types = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            types.add(DataType.DOUBLE);
        }
        long startTime = System.currentTimeMillis();
        System.out.printf("Session insertNonAlignedRowRecords start time: %s%n", startTime);
        session.insertNonAlignedRowRecords(path, timestamps, values, types, null);
        long endTime = System.currentTimeMillis();
        System.out.printf("Session insertNonAlignedRowRecords end time: %s%n", endTime);
        System.out.printf("Total insert time: %s%n", endTime - startTime);
    }

    private static void testInsertRowRecords(long time, int pathNum) throws ExecutionException, SessionException {
        List<String> path = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            path.add("1001.SUCC" + i);
        }
        long[] timestamps = new long[1];
        timestamps[0] = time;
        Object[] values = new Object[1];
        List<Object> valueList = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            valueList.add(323.0);
        }
        values[0] = valueList.toArray();
        List<DataType> types = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            types.add(DataType.DOUBLE);
        }
        long startTime = System.currentTimeMillis();
        System.out.printf("Session insertRowRecords start time: %s%n", startTime);
        session.insertRowRecords(path, timestamps, values, types, null);
        long endTime = System.currentTimeMillis();
        System.out.printf("Session insertRowRecords end time: %s%n", endTime);
        System.out.printf("Total insert time: %s%n", endTime - startTime);
    }

    private static void testInsertNonAlignedColumnRecords(long time, int pathNum) throws ExecutionException, SessionException {
        List<String> path = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            path.add("1001.SUCC" + i);
        }
        long[] timestamps = new long[1];
        timestamps[0] = time;
        Object[] values = new Object[pathNum];
        for (int i = 0; i < pathNum; i++) {
            values[i] = new Object[]{323.0};
        }
        List<DataType> types = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            types.add(DataType.DOUBLE);
        }
        long startTime = System.currentTimeMillis();
        System.out.printf("Session insertNonAlignedColumnRecords start time: %s%n", startTime);
        session.insertNonAlignedColumnRecords(path, timestamps, values, types, null);
        long endTime = System.currentTimeMillis();
        System.out.printf("Session insertNonAlignedColumnRecords end time: %s%n", endTime);
        System.out.printf("Total insert time: %s%n", endTime - startTime);
    }

    private static void testInsertColumnRecords(long time, int pathNum) throws ExecutionException, SessionException {
        List<String> path = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            path.add("1001.SUCC" + i);
        }
        long[] timestamps = new long[1];
        timestamps[0] = time;
        Object[] values = new Object[pathNum];
        for (int i = 0; i < pathNum; i++) {
            values[i] = new Object[]{323.0};
        }
        List<DataType> types = new ArrayList<>();
        for (int i = 0; i < pathNum; i++) {
            types.add(DataType.DOUBLE);
        }
        long startTime = System.currentTimeMillis();
        System.out.printf("Session insertColumnRecords start time: %s%n", startTime);
        session.insertColumnRecords(path, timestamps, values, types, null);
        long endTime = System.currentTimeMillis();
        System.out.printf("Session insertColumnRecords end time: %s%n", endTime);
        System.out.printf("Total insert time: %s%n", endTime - startTime);
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

        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertNonAlignedColumnRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

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
                    if (i < 2) {
                        values[(int) j] = i + j;
                    } else {
                        values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                    }
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

        session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = ROW_START_TIMESTAMP + i;
            Object[] values = new Object[4];
            for (long j = 0; j < 4; j++) {
                if (j < 2) {
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

        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertNonAlignedRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (NON_ALIGNED_ROW_END_TIMESTAMP - NON_ALIGNED_ROW_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = NON_ALIGNED_ROW_START_TIMESTAMP + i;
            Object[] values = new Object[4];
            for (long j = 0; j < 4; j++) {
                if ((i + j) % 2 == 0) {
                    values[(int) j] = null;
                } else {
                    if (j < 2) {
                        values[(int) j] = i + j;
                    } else {
                        values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                    }
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

        session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void queryData() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        dataSet.print();
    }

    private static void valueFilterQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;
        String booleanExpression = S2 + " < " + 9930 + " && " + S1 + " > " + 9910;
        SessionQueryDataSet dataSet = session.valueFilterQuery(paths, startTime, endTime, booleanExpression);
        dataSet.print();
    }

    private static void aggregateQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = NON_ALIGNED_ROW_START_TIMESTAMP + 100L;

        // 聚合查询开始
        System.out.println("Aggregate Query: ");

        // MAX
        SessionAggregateQueryDataSet dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MAX);
        dataSet.print();

        // MIN
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MIN);
        dataSet.print();

        // FIRST_VALUE
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST_VALUE);
        dataSet.print();

        // LAST_VALUE
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.LAST_VALUE);
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

        // 聚合查询结束
        System.out.println("Aggregate Query Finished.");
    }

    private static void lastQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        LastQueryDataSet dataSet = session.queryLast(paths, 0L);
        dataSet.print();
    }

    private static void downsampleQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = NON_ALIGNED_ROW_START_TIMESTAMP + 100L;

        // 降采样查询开始
        System.out.println("Downsample Query: ");

        // MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, INTERVAL * 100L);
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, INTERVAL * 100L);
        dataSet.print();

        // FIRST_VALUE
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST_VALUE, INTERVAL * 100L);
        dataSet.print();

        // LAST_VALUE
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST_VALUE, INTERVAL * 100L);
        dataSet.print();

        // COUNT
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.COUNT, INTERVAL * 100L);
        dataSet.print();

        // SUM
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.SUM, INTERVAL * 100L);
        dataSet.print();

        // AVG
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.AVG, INTERVAL * 100L);
        dataSet.print();

        // 降采样查询结束
        System.out.println("Downsample Query Finished.");
    }

    private static void deleteDataInColumns() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S3);
        paths.add(S4);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 50L;
        long endTime = ROW_START_TIMESTAMP + 50L;

        session.deleteDataInColumns(paths, startTime, endTime);
    }
}
