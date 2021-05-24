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
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.List;

public class TDEngineExample {

    private static Session session;

    private static final String S1 = "sg1.d1.s1";
    private static final String S2 = "sg1.d2.s2";

    private static final long startTimestamp = System.currentTimeMillis();

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");

        session.openSession();

        // 写数据
        insertRowRecords();

        // 插数据
        queryData();

        session.closeSession();
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        int size = 1000; // 写入 1000 个数据点

        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (int i = 0; i < size; i++) {
            timestamps[i] = startTimestamp + i * 1000;
            Object[] values = new Object[2];
            for (int j = 0; j < values.length; j++) {
                values[j] = RandomStringUtils.randomAlphanumeric(1024).getBytes();
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void queryData() throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);

        long startTime = startTimestamp;
        long endTime = startTime + 1001;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        dataSet.print();
    }

}
