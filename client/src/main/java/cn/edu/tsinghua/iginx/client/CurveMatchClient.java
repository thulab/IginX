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
package cn.edu.tsinghua.iginx.client;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.CurveMatchResult;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CurveMatchClient {

    private static Session session;

    public static void main(String[] args) throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        // 曲线匹配
        curveMatch(args);

        // 关闭 Session
        session.closeSession();
    }

    private static void curveMatch(String[] args) throws ExecutionException, SessionException {
        if (args.length != 5) {
            System.out.println("参数个数必须为5！");
            return;
        }

        List<String> paths = Arrays.stream(args[0].split(";")).collect(Collectors.toList());

        long startTime = Long.parseLong(args[1]);
        long endTime = Long.parseLong(args[2]);

        List<Double> queryList = Arrays.stream(args[3].split(",")).map(Double::parseDouble).collect(Collectors.toList());

        long curveUnit = Long.parseLong(args[4]);

        CurveMatchResult result = session.curveMatch(paths, startTime, endTime, queryList, curveUnit);

        long matchedTimestamp = result.getMatchedTimestamp();
        String matchedPath = result.getMatchedPath();

        String[] parts = matchedPath.split("\\.");
        StringBuilder sql = new StringBuilder();
        sql.append("select first_value(");
        sql.append(parts[parts.length - 1]);
        sql.append(") from ");
        sql.append(matchedPath, 0, matchedPath.lastIndexOf('.'));
        sql.append(" group (");
        sql.append(matchedTimestamp);
        sql.append(", ");
        sql.append(matchedTimestamp + curveUnit * queryList.size());
        sql.append(") by ");
        sql.append(curveUnit);
        sql.append("ms");

        SessionExecuteSqlResult dataSet = session.executeSql(sql.toString());
        dataSet.print(false, "ms");
    }
}
