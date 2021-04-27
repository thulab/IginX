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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBDownsampleExample {


    private static Session session;

    private static final String DATABASE_NAME = "sg1";
    private static final String S1 = "sg1.d1.s1";
    private static final String S2 = "sg1.d2.s2";
    private static final String S3 = "sg1.d3.s3";
    private static final String S4 = "sg1.d4.s4";

    private static final long START_TIME = 0;
    private static final long END_TIME = 100;

    private static final int INSERT_INTERVAL = 1;

    private static final long PRECISION = 7L;

    private static final List<String> paths = new ArrayList<>();

    static {
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);
    }

    public static void main(String[] args) throws Exception {
        session = new Session("127.0.0.1", 6324, "root", "root");
        // 打开 Session
        session.openSession();

        // 创建数据库
        session.createDatabase(DATABASE_NAME);

        // 添加列
        addColumns();

        // 插入前查询数据
        downsampleQuery();

        // 插入数据
        insertData();

        // 查询数据
        downsampleQuery();

        // 删除数据
        deleteData();

        // 再次查询数据
        downsampleQuery();

        session.closeSession();
    }

    private static void addColumns() throws SessionException, ExecutionException {
        List<Map<String, String>> attributes = new ArrayList<>();
        Map<String, String> attributesForOnePath = new HashMap<>();

        // INT64
        attributesForOnePath.put("DataType", "2");
        // RLE
        attributesForOnePath.put("Encoding", "2");
        // SNAPPY
        attributesForOnePath.put("Compression", "1");

        for (int i = 0; i < paths.size(); i++) {
            attributes.add(attributesForOnePath);
        }
        session.addColumns(paths, attributes);
    }

    private static void insertData() throws SessionException, ExecutionException {
        int size = (int) (END_TIME - START_TIME) / INSERT_INTERVAL;
        long[] timestamps = new long[size];

        for (long i = 0; i < size; i++) {
            //timestamps[(int) i] = START_TIME + i * INSERT_INTERVAL;
            timestamps[(int) i] = i;
        }

        Object[] valuesList = new Object[paths.size()];
        for (long i = 0; i < paths.size(); i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                if (i < 2) {
                    values[(int) j] = i + j;
                } else {
                    values[(int) j] = j + i * i;
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            dataTypeList.add(DataType.LONG);
        }

        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);

    }

    private static void downsampleQuery() throws SessionException, ExecutionException {

        long startTime = START_TIME;
        long endTime = END_TIME + 1;

        System.out.println("Downsample Query: ");

        // MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, PRECISION);
        dataSet.print();

         // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, PRECISION);
        dataSet.print();

        // FIRST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST, PRECISION);
        dataSet.print();

        // LAST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST, PRECISION);
        dataSet.print();

        // COUNT
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.COUNT, PRECISION);
        dataSet.print();

        // SUM
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.SUM, PRECISION);
        dataSet.print();

        // AVG
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.AVG, PRECISION);
        dataSet.print();

        // 降采样查询结束
        System.out.println("Downsample Query Finished.");
        System.out.println();
    }

    private static void deleteData() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        long startTime = START_TIME;
        long endTime = END_TIME + 1;
        session.deleteDataInColumns(paths, startTime, endTime);
    }


}
