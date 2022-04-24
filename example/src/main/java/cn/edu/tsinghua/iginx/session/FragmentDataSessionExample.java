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

public class FragmentDataSessionExample {

  IginXClient client = IginXClientFactory.create();

  public static void main(String[] args) {
    new FragmentDataSessionExample().start();
  }

  void start() {
    new Thread(new ReadThread("a.b.c", 1650716382237L, Long.MAX_VALUE)).start();
    new Thread(new ReadThread("c.b.a", 1650716382237L, Long.MAX_VALUE)).start();
  }

  class ReadThread implements Runnable {

    private String measurement;
    private long startTime;
    private long endTime;

    public ReadThread(String measurement, long startTime, long endTime) {
      this.measurement = measurement;
      this.startTime = startTime;
      this.endTime = endTime;
    }

    @Override
    public void run() {
      QueryClient queryClient = client.getQueryClient();
      IginXTable table = queryClient.query(
          SimpleQuery.builder()
              .addMeasurement(measurement)
              .startTime(startTime)
              .endTime(endTime)
              .build()
      );
      for (IginXRecord record : table.getRecords()) {
        System.out.println(record);
      }
    }
  }
}