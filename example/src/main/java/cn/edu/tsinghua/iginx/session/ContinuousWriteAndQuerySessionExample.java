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
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import cn.edu.tsinghua.iginx.session_v2.write.Point;

public class ContinuousWriteAndQuerySessionExample {

  IginXClient client = IginXClientFactory.create();
  long startTime = System.currentTimeMillis();

  public static void main(String[] args) {
    new ContinuousWriteAndQuerySessionExample().start();
  }

  void start() {
//    new Thread(new WriteThread("b.c.a", 5)).start();
//    new Thread(new WriteThread("a.b.c", 10)).start();
//    new Thread(new WriteThread("c.b.a", 20)).start();
    new Thread(new ReadThread("b.c.a", 1)).start();
    new Thread(new ReadThread("a.b.c", 1)).start();
    new Thread(new ReadThread("c.b.a", 1)).start();
    new Thread(new ReadThread("d.b.a", 1)).start();
    new Thread(new ReadThread("e.b.a", 1)).start();
  }

  class WriteThread implements Runnable {

    private String measurement;
    private long sleepTime;

    public WriteThread(String measurement, long sleepTime) {
      this.measurement = measurement;
      this.sleepTime = sleepTime;
    }

    @Override
    public void run() {
      WriteClient writeClient = client.getWriteClient();
      while (true) {
        try {
          Thread.sleep(sleepTime);
          long time = System.currentTimeMillis();
          writeClient.writePoint(
              Point.builder()
                  .timestamp(time)
                  .measurement(measurement)
                  .longValue(time)
                  .build()
          );
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  class ReadThread implements Runnable {

    private String measurement;
    private long sleepTime;

    public ReadThread(String measurement, long sleepTime) {
      this.measurement = measurement;
      this.sleepTime = sleepTime;
    }

    @Override
    public void run() {
      QueryClient queryClient = client.getQueryClient();
      while (true) {
        try {
          Thread.sleep(sleepTime);
          IginXTable table = queryClient.query(
              SimpleQuery.builder()
                  .addMeasurement(measurement)
                  .startTime(startTime)
                  .endTime(startTime + 2000L)
                  .build()
          );
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}