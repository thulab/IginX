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

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IoTDBTagExample {

    private static final List<String> measurements = new ArrayList<>();

    private static final List<Map<String, String>> tagsList = new ArrayList<>();

    private static final long COLUMN_START_TIMESTAMP = 0L;

    private static final long COLUMN_END_TIMESTAMP = 100L;

    private static final long ROW_START_TIMESTAMP = 200L;

    private static final long ROW_END_TIMESTAMP = 300L;

    static {
        measurements.add("usage.memory");
        measurements.add("usage.cpu");
        measurements.add("usage.disk");

        Map<String, String> tags1 = new HashMap<>();
        tags1.put("host", "1");
        tags1.put("center", "beijing");
        tagsList.add(tags1);

        Map<String, String> tags2 = new HashMap<>();
        tags2.put("host", "2");
        tags2.put("center", "beijing");
        tagsList.add(tags2);

        Map<String, String> tags3 = new HashMap<>();
        tags3.put("host", "1");
        tags3.put("center", "shanghai");
        tagsList.add(tags3);
    }

    private static List<Pair<String, Map<String, String>>> generatePathAndTagsList() {
        List<Pair<String, Map<String, String>>> pathAndTagsList = new ArrayList<>();
//        for (String measurement : measurements) {
//            for (Map<String, String> stringStringMap : tagsList) {
//                pathAndTagsList.add(new Pair<>(measurement, new HashMap<>(stringStringMap)));
//            }
//        }
        for (int i = 0; i < measurements.size(); i++) {
            pathAndTagsList.add(new Pair<>(measurements.get(i), tagsList.get(i)));
        }
        return pathAndTagsList;
    }

    private static Session session;

    public static void main(String[] args) throws Exception {
        session = new Session("127.0.0.1", 6888, "root", "root");

        session.openSession();

        insertRowRecords();

        insertColumnRecords();

        queryData();

        session.closeSession();
    }

    private static void insertColumnRecords() throws Exception {
        List<Pair<String, Map<String, String>>> pathAndTagsList = generatePathAndTagsList();

        long[] timestamps = new long[(int)(COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP)];
        for (long i = COLUMN_START_TIMESTAMP; i < COLUMN_END_TIMESTAMP; i++) {
            timestamps[(int)(i - COLUMN_START_TIMESTAMP)] = i;
        }

        Object[] valuesList = new Object[pathAndTagsList.size()];
        for (int i = 0; i < valuesList.length; i++) {
            Object[] values = new Object[timestamps.length];
            for (int j = 0; j < values.length; j++) {
                values[j] = (long)(i + j + COLUMN_START_TIMESTAMP);
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < pathAndTagsList.size(); i++) {
            dataTypeList.add(DataType.LONG);
        }

        session.insertColumnRecords(pathAndTagsList.stream().map(e -> e.k).collect(Collectors.toList()), timestamps, valuesList, dataTypeList,
                pathAndTagsList.stream().map(e -> e.v).collect(Collectors.toList()));
    }

    private static void insertRowRecords() throws Exception {
        List<Pair<String, Map<String, String>>> pathAndTagsList = generatePathAndTagsList();

        long[] timestamps = new long[(int)(ROW_END_TIMESTAMP - ROW_START_TIMESTAMP)];
        for (long i = ROW_START_TIMESTAMP; i < ROW_END_TIMESTAMP; i++) {
            timestamps[(int)(i - ROW_START_TIMESTAMP)] = i;
        }

        Object[] valuesList = new Object[timestamps.length];
        for (int i = 0; i < valuesList.length; i++) {
            Object[] values = new Object[pathAndTagsList.size()];
            for (int j = 0; j < values.length; j++) {
                values[j] = (long)(i + j + ROW_START_TIMESTAMP);
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < pathAndTagsList.size(); i++) {
            dataTypeList.add(DataType.LONG);
        }

        session.insertRowRecords(pathAndTagsList.stream().map(e -> e.k).collect(Collectors.toList()), timestamps, valuesList, dataTypeList,
                pathAndTagsList.stream().map(e -> e.v).collect(Collectors.toList()));
    }

    private static void queryData() throws Exception {
        List<Pair<String, Map<String, String>>> pathAndTagsList = generatePathAndTagsList();
        long startTime = COLUMN_START_TIMESTAMP + ((COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP) >> 1);
        long endTime = ROW_END_TIMESTAMP - ((ROW_END_TIMESTAMP - ROW_START_TIMESTAMP) >> 1);

        System.out.println("query range from " + startTime + " to " + endTime);

        SessionQueryDataSet dataSet = session.queryData(pathAndTagsList.stream().map(e -> e.k).collect(Collectors.toList()), pathAndTagsList.stream().map(e -> e.v).collect(Collectors.toList()),
                startTime, endTime);
        dataSet.print();
    }


}
