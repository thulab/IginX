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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class IoTDBInsertRowRecordsExample {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBInsertRowRecordsExample.class);

    private static Session session;

    private static final String DATABASE_NAME = "root.sg1";
    private static final String COLUMN_D1_S1 = "root.sg1.d1.s1";
    private static final String COLUMN_D1_S2 = "root.sg1.d2.s2";
    private static final String COLUMN_D2_S1 = "root.sg1.d3.s1";
    private static final String COLUMN_D3_S1 = "root.sg1.d4.s1";

    private static final List<String> paths = new ArrayList<>();

    static {
        paths.add(COLUMN_D1_S1);
        paths.add(COLUMN_D1_S2);
        paths.add(COLUMN_D2_S1);
        paths.add(COLUMN_D3_S1);
    }

    private static final long beginTimestamp = 1000L;

    private static final long endTimestamp = 10000L;

    private static final int interval = 10;

    public static void main(String[] args) throws Exception {
        session = new Session("127.0.0.1", 6324, "root", "root");
        // 开启 session
        session.openSession();
        // 创建数据库
        session.createDatabase(DATABASE_NAME);
        // 增加时序列
        addColumns();
        // 插入数据
        insertRowRecords();
        // 查询数据
        queryData();
        // 关闭 session
        session.closeSession();
    }

    private static void addColumns() throws SessionException, ExecutionException {
        Map<String, String> attributesForOnePath = new HashMap<>();
        // TEXT
        attributesForOnePath.put("DataType", "5");
        // PLAIN
        attributesForOnePath.put("Encoding", "0");
        // SNAPPY
        attributesForOnePath.put("Compression", "1");

        List<Map<String, String>> attributes = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            attributes.add(attributesForOnePath);
        }
        session.addColumns(paths, attributes);
    }

    private static void insertRowRecords() throws SessionException, ExecutionException {
        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            dataTypeList.add(DataType.BINARY);
        }
        int size = (int)(endTimestamp - beginTimestamp) / interval;
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            timestamps[i] = beginTimestamp + i * interval;
            Object[] values = new Object[paths.size()];
            for (int j = 0; j < paths.size(); j++) {
                if (random.nextInt() % 2 == 0) {
                    values[j] = null;
                } else {
                    values[j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[i] = values;
        }
        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void queryData() throws SessionException {
        List<String> queryPaths = new ArrayList<>();
        queryPaths.add("root.sg1.d1.*");
        queryPaths.add("root.sg1.d2.*");
        queryPaths.add("root.sg1.d3.*");
        SessionQueryDataSet dataSet = session.queryData(queryPaths, beginTimestamp, endTimestamp + 1);
        dataSet.print();
    }
}
