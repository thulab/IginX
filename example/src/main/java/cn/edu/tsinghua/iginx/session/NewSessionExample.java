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


import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientFactory;
import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import cn.edu.tsinghua.iginx.session_v2.write.Point;

import java.util.Arrays;

public class NewSessionExample {

    public static void main(String[] args) {
        IginXClient client = IginXClientFactory.create();
        WriteClient writeClient = client.getWriteClient();
        writeClient.writePoint(
                Point.builder()
                        .now()
                        .measurement("a.a.a")
                        .intValue(2333)
                        .build()
        );
        writeClient.writePoint(
                Point.builder()
                        .key(System.currentTimeMillis() - 1000L)
                        .measurement("a.b.b")
                        .doubleValue(2333.2)
                        .build()
        );

        long timestamp = System.currentTimeMillis();
        Point point1 = Point.builder()
                .key(timestamp)
                .measurement("a.a.a")
                .intValue(666)
                .build();
        Point point2 = Point.builder()
                .key(timestamp)
                .measurement("a.b.b")
                .doubleValue(666.0)
                .build();
        writeClient.writePoints(Arrays.asList(point1, point2));

        QueryClient queryClient = client.getQueryClient();
        IginXTable table = queryClient.query(
                SimpleQuery.builder()
                        .addMeasurement("a.a.a")
                        .addMeasurement("a.b.b")
                        .endTime(System.currentTimeMillis() + 1000L)
                        .build()
        );
        System.out.println("Header:" + table.getHeader());

        for (IginXRecord record: table.getRecords()) {
            System.out.println(record);
        }

        client.close();
    }

}
