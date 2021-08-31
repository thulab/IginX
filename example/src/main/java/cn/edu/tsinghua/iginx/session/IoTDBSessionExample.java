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

public class IoTDBSessionExample {

    private static final String S1 = "sg.d1.s1";
    private static final String S2 = "sg.d2.s2";
    private static final String S3 = "sg.d3.s3";
    private static final String S4 = "sg.d4.s4";
    private static final long COLUMN_START_TIMESTAMP = 0L;
    private static final long COLUMN_END_TIMESTAMP = 10500L;
    private static final long ROW_START_TIMESTAMP = 10501L;
    private static final long ROW_END_TIMESTAMP = 21000L;
    private static final int ROW_INTERVAL = 10;
    private static Session session;

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        // 列式插入数据
        insertColumnRecords();
        // 行式插入数据
        insertRowRecords();
        // 值过滤查询
        valueFilterQuery();
        // 查询数据
        queryData();
        // 聚合查询数据
        aggregateQuery();
        // 降采样聚合查询
        downsampleQuery();
        // 删除数据
        deleteDataInColumns();
        // 再次查询数据
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

        int size = (int) (COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP);
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i;
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

        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP) / ROW_INTERVAL;
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = ROW_START_TIMESTAMP + i * ROW_INTERVAL;
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

        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void queryData() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
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

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;
        String booleanExpression = S2 + " < " + (ROW_START_TIMESTAMP + 50L) + " && " + S1 + " > " + (COLUMN_END_TIMESTAMP - 50L);
        SessionQueryDataSet dataSet = session.valueFilterQuery(paths, startTime, endTime, booleanExpression);
        dataSet.print();
    }

    private static void downsampleQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

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
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST_VALUE, ROW_INTERVAL * 100);
        dataSet.print();

        // LAST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST_VALUE, ROW_INTERVAL * 100);
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

    private static void aggregateQuery() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;

        // MAX
        SessionAggregateQueryDataSet dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MAX);
        dataSet.print();

        // MIN
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MIN);
        dataSet.print();

        // FIRST
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST_VALUE);
        dataSet.print();

        // LAST
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
}
