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
import java.util.Arrays;
import java.util.List;

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

        // 列式插入对齐数据
        insertColumnRecords();
        // 列式插入非对齐数据
        insertNonAlignedColumnRecords();
        // 行式插入对齐数据
        insertRowRecords();
        // 行式插入非对齐数据
        insertNonAlignedRowRecords();
        // 查询序列
        showTimeSeries();
        // 查询数据
        queryData();
        // 聚合查询
        aggregateQuery();
        // Last 查询
        lastQuery();
        // 降采样聚合查询
        downsampleQuery();
        // 曲线匹配
        curveMatch();
        // 删除数据
        deleteDataInColumns();
        // 再次查询数据
        queryData();
        // 查看集群信息
        showClusterInfo();

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

        System.out.println("insertColumnRecords...");
        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null, "ns");
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

        System.out.println("insertNonAlignedColumnRecords...");
        session.insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, null, "ns");
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

        System.out.println("insertRowRecords...");
        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null, "ns");
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

        System.out.println("insertNonAlignedRowRecords...");
        session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, null, "ns");
    }

    private static void showTimeSeries() throws ExecutionException, SessionException {
        List<Column> columnList = session.showColumns();
        columnList.forEach(column -> System.out.println(column.toString()));
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

        dataSet = session.queryData(paths, startTime, endTime, null, "ms");
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
        SessionAggregateQueryDataSet dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MAX, "ns");
        dataSet.print();

        // MIN
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MIN, "ns");
        dataSet.print();

        // FIRST_VALUE
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST_VALUE, "ns");
        dataSet.print();

        // LAST_VALUE
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.LAST_VALUE, "ns");
        dataSet.print();

        // COUNT
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.COUNT, "ns");
        dataSet.print();

        // SUM
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.SUM, "ns");
        dataSet.print();

        // AVG
        dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.AVG, "ns");
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

        SessionQueryDataSet dataSet = session.queryLast(paths, 0L);
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
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, INTERVAL * 100L, "ns");
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, INTERVAL * 100L, "ns");
        dataSet.print();

        // FIRST_VALUE
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST_VALUE, INTERVAL * 100L, "ns");
        dataSet.print();

        // LAST_VALUE
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST_VALUE, INTERVAL * 100L, "ns");
        dataSet.print();

        // COUNT
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.COUNT, INTERVAL * 100L, "ns");
        dataSet.print();

        // SUM
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.SUM, INTERVAL * 100L, "ns");
        dataSet.print();

        // AVG
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.AVG, INTERVAL * 100L, "ns");
        dataSet.print();

        // 降采样查询结束
        System.out.println("Downsample Query Finished.");
    }

    private static void curveMatch() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = COLUMN_END_TIMESTAMP - 100L;
        long endTime = NON_ALIGNED_ROW_START_TIMESTAMP + 100L;

        double bias = 6.0;
        int queryNum = 30;
        List<Double> queryList = new ArrayList<>();
        for (int i = 0; i < queryNum; i++) {
            queryList.add(startTime + bias + i);
        }
        long curveUnit = 1L;

        CurveMatchResult result = session.curveMatch(paths, startTime, endTime, queryList, curveUnit);
        System.out.println(result.toString());
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

    public static void showClusterInfo() throws SessionException, ExecutionException {
        ClusterInfo clusterInfo = session.getClusterInfo();
        System.out.println(clusterInfo.getIginxInfos());
        System.out.println(clusterInfo.getStorageEngineInfos());
        System.out.println(clusterInfo.getMetaStorageInfos());
        System.out.println(clusterInfo.getLocalMetaStorageInfo());
    }
}
