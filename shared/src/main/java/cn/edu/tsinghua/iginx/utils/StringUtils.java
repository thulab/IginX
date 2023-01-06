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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    /**
     * @return        返回值为 0 表示包含，>0 表示在这个序列在 border 前，<0 表示 ts 在 border 后
     * @param ts      时间序列(可能等于/含有*，不可能为null)
     * @param border  前缀式时间范围
     */
    public static int compare(String ts, String border) {
        if (ts.indexOf(border) == 0) {
            return 0;
        }
        else
            return ts.compareTo(border);
    }

    public static String nextString(String str) {
        return str.substring(0, str.length() - 1) + (char)(str.charAt(str.length() - 1) + 1);
    }

    public static boolean allHasMoreThanOneSubPath(List<String> pathList) {
        for (String path : pathList) {
            if (!hasMoreThanOneSubPath(path)) {
                return false;
            }
        }
        return true;
    }

    public static boolean hasMoreThanOneSubPath(String path) {
        return path.contains(".");
    }

    public static boolean isPattern(String path) {
        return path.contains("*");
    }

    public static List<String> reformatPaths(List<String> paths) {
        List<String> ret = new ArrayList<>();
        paths.forEach(path -> ret.add(reformatPath(path)));
        return ret;
    }

    public static String reformatPath(String path) {
        if (!path.contains("*"))
            return path;
        path = path.replaceAll("[.]", "[.]");
        path = path.replaceAll("[*]", ".*");
        return path;
    }

    public static String reformatColumnName(String name) {
        if (!name.contains("*") && !name.contains("(") && !name.contains(")"))
            return name;
        name = name.replaceAll("[.]", "[.]");
        name = name.replaceAll("[*]", ".*");
        name = name.replaceAll("[(]", "[(]");
        name = name.replaceAll("[)]", "[)]");
        return name;
    }

    public static boolean isContainSpecialChar(String str) {
        String regEx = "[ _`~!@#$%^&*()+=|{}':;',\\[\\]<>/?~！@#￥%……&*（）+|{}【】‘；：”“’。，、？]|\n|\r|\t";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.find();
    }
}
