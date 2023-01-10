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
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.annotations.Field;
import cn.edu.tsinghua.iginx.session_v2.annotations.Measurement;
import cn.edu.tsinghua.iginx.session_v2.write.Point;
import cn.edu.tsinghua.iginx.session_v2.write.Record;
import cn.edu.tsinghua.iginx.session_v2.write.Table;
import cn.edu.tsinghua.iginx.thrift.DataType;

/**
 * Created on 10/12/2021.
 * Description: 暂时只是样例，并不能实际运行
 *
 * @author ziyuan
 */
public class NewSessionNotSupportedWriteExample {

    public static void main(String[] args) {

        IginXClient client = IginXClientFactory.create();
        WriteClient writeClient = client.getWriteClient();

        // 写入单个数据点
        writeClient.writePoint(
                Point.builder()
                        .now()
                        .measurement("a.a.a")
                        .intValue(2333)
                        .build()
        );

        System.out.println("写入数据点成功");

        // 写入一些有相同时间戳的数据点
        writeClient.writeRecord(
                Record.builder()
                        .key(System.currentTimeMillis())
                        .measurement("a.b")
                        .addIntField("a", 100)
                        .addLongField("b", 1000L)
                        .addFloatField("c", 23.33F)
                        .addDoubleField("d", 233.3)
                        .addBinaryField("e", "hello, world".getBytes())
                        .build()
        );

        System.out.println("写入行成功");

        // 写入一张二维表
        writeClient.writeTable(
                Table.builder()
                        .measurement("a.c")
                        .addField("a", DataType.INTEGER)
                        .addField("b", DataType.LONG) // 设置二维表中有哪些列
                        .key(System.currentTimeMillis()) // 设置第一行的时间戳
                        .intValue("a", 1000) // 为第一行追加数据
                        .longValue("b", 232333L) // 为第一行追加数据
                        .next() // 开启新的一行
                        .key(System.currentTimeMillis() + 1000) // 设置第二行时间戳
                        .value("b", 200000L) // 为第二行追加数据
                        .build()
        );

        System.out.println("写入二维表成功");

        // 写入数据对象
        POJO pojo = new POJO(System.currentTimeMillis(), 10, 11);
        writeClient.writeMeasurement(pojo); // 实际会将对象转化为两个序列：demo.pojo.a 和 demo.pojo.b

        System.out.println("写入数据对象成功");

        client.close();
    }


    @Measurement(name = "demo.pojo")
    static class POJO {

        @Field(timestamp = true)
        long timestamp;

        @Field
        int a;

        @Field
        int b;

        POJO(long timestamp, int a, int b) {
            this.timestamp = timestamp;
            this.a = a;
            this.b = b;
        }
    }


}

