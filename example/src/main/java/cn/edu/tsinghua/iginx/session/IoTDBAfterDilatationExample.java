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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IoTDBAfterDilatationExample {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBAfterDilatationExample.class);
    private static final String COLUMN_C1_S1 = "sg.c1.s1";
    private static final String COLUMN_C2_S2 = "sg.c2.s2";
    private static final String COLUMN_E2_S1 = "sg.e2.s1";
    private static final String COLUMN_E3_S1 = "sg.e3.s1";
    private static final long beginTimestamp = 1000000000L;
    private static final int insertTimes = 1000;
    private static final int recordPerInsert = 100;
    private static final List<String> paths = new ArrayList<>();
    private static Session session;

    static {
        paths.add(COLUMN_C1_S1);
        paths.add(COLUMN_C2_S2);
        paths.add(COLUMN_E2_S1);
        paths.add(COLUMN_E3_S1);
    }

    public static void main(String[] args) throws Exception {
        session = new Session("127.0.0.1", 6888, "root", "root");
        session.openSession();
        // 插入数据
        insertRecords();
        // 关闭 session
        session.closeSession();
    }

    private static void insertRecords() throws SessionException, ExecutionException, InterruptedException {
        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            dataTypeList.add(DataType.LONG);
        }

        for (int i = 0; i < insertTimes; i++) {
            long[] timestamps = new long[recordPerInsert];
            Object[] valuesList = new Object[paths.size()];
            for (int j = 0; j < recordPerInsert; j++) {
                timestamps[j] = beginTimestamp + (long) i * recordPerInsert + j;
            }
            for (int k = 0; k < paths.size(); k++) {
                Object[] values = new Object[recordPerInsert];
                for (int j = 0; j < recordPerInsert; j++) {
                    values[j] = (long) (i + j + k);
                }
                valuesList[k] = values;
            }
            session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
            Thread.sleep(1);
            if ((i + 1) % 10 == 0) {
                logger.info("insert progress: " + (i + 1) + "/1000.");
            }
        }
    }

}
