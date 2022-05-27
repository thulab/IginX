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
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;

public class TimescaleDBSessionExample {

    private static final String S1 = "sg.d1.s1";
    private static final String S2 = "sg.d1.s2";
    private static final String S3 = "sg.d2.s3";
    private static final String S4 = "sg.d3.s4";
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
        showTimeseries();
        // 行式插入对齐数据
//        insertRowRecords();
//        queryData();
//        deleteDataInColumns();
        // 关闭 Session
        session.closeSession();
    }

    private static void showTimeseries() throws SessionException, ExecutionException {
        List<Column> columns = session.showColumns();
        for(Column column: columns){
            System.out.println(column.getPath());
        }
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
//        paths.add(S3);
//        paths.add(S4);

        int size = (int) (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP + 1);
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = ROW_START_TIMESTAMP + i;
            Object[] values = new Object[2];
            for (long j = 0; j < 4; j++) {
                if (j < 2) {
                    values[(int) j] = i + j;
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }

        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void queryData() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
//        paths.add(S3);
//        paths.add(S4);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 100L;
        long endTime = ROW_START_TIMESTAMP + 100L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        dataSet.print();
    }

    private static void deleteDataInColumns() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = NON_ALIGNED_COLUMN_END_TIMESTAMP - 50L;
        long endTime = ROW_START_TIMESTAMP + 50L;

        session.deleteDataInColumns(paths, startTime, endTime);
    }
}
