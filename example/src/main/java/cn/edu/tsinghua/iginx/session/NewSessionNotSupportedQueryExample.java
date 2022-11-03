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
import cn.edu.tsinghua.iginx.session_v2.annotations.Field;
import cn.edu.tsinghua.iginx.session_v2.annotations.Measurement;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;

import java.util.List;

/**
 * Created on 10/12/2021.
 * Description: 暂时只是样例，并不能实际运行
 *
 * @author ziyuan
 */
public class NewSessionNotSupportedQueryExample {

    public static void main(String[] args) {

        IginXClient client = IginXClientFactory.create();
        QueryClient queryClient = client.getQueryClient();

        IginXTable table = queryClient.query( // 查询 a.a.a 序列最近一秒内的数据
                SimpleQuery.builder()
                        .addMeasurement("a.a.a")
                        .startTime(System.currentTimeMillis() - 1000L)
                        .endTime(System.currentTimeMillis())
                        .build()
        );
        List<POJO> pojoList = queryClient.query("select * from demo.pojo where time < now() and time > now() - 1000", POJO.class); // 查询最近一秒内的 pojo 对象
        client.close(); // orm
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

