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
package cn.edu.tsinghua.iginx.utils;

public class StringUtils {

    /**
     * @param ts      时间序列(可能等于/含有*，不可能为null)
     * @param border  分片的开始/结束边界(不可能等于/含有*，可能为null)
     * @param isStart 是否为开始边界
     */
    public static int compare(String ts, String border, boolean isStart) {
        if (border == null) {
            return isStart ? 1 : -1;
        }
        if (ts.equals("*")) {
            return isStart ? 1 : -1;
        }
        if (ts.contains("*")) {
            String p1 = ts.substring(0, ts.indexOf("*"));
            if (border.equals(p1)) {
                return 1;
            }
            if (border.startsWith(p1)) {
                return isStart ? 1 : -1;
            }
            return p1.compareTo(border);
        } else {
            return ts.compareTo(border);
        }
    }
}
