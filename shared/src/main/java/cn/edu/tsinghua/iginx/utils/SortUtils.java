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

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedColumnRecordsReq;
import org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

public class SortUtils {

    // 适用于查询类请求和删除类请求，因为其 paths 可能带有 *
    public static List<String> mergeAndSortPaths(List<String> paths) {
        if (paths.stream().anyMatch(x -> x.equals("*"))) {
            List<String> tempPaths = new ArrayList<>();
            tempPaths.add("*");
            return tempPaths;
        }
        List<String> prefixes = paths.stream().filter(x -> x.contains("*")).map(x -> x.substring(0, x.indexOf("*"))).collect(Collectors.toList());
        if (prefixes.isEmpty()) {
            Collections.sort(paths);
            return paths;
        }
        List<String> mergedPaths = new ArrayList<>();
        for (String path : paths) {
            if (path.contains("*")) {
                mergedPaths.add(path);
            } else {
                boolean skip = false;
                for (String prefix : prefixes) {
                    if (path.startsWith(prefix)) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }
                mergedPaths.add(path);
            }
        }
        mergedPaths.sort(Comparator.comparing(o -> o.contains("*") ? o.substring(0, o.indexOf("*")) : o));
        return mergedPaths;
    }
}
