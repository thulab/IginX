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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TagKVUtils {

    public static String toFullName(String name, Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return name;
        } else {
            StringBuilder builder = new StringBuilder();
            builder.append(name);
            builder.append('{');
            TreeMap<String, String> treeMap = new TreeMap<>(tags);

            int cnt = 0;
            for (String key: treeMap.keySet()) {
                if (cnt != 0) {
                    builder.append(',');
                }
                builder.append(key);
                builder.append("=");
                builder.append(treeMap.get(key));
                cnt++;
            }
            builder.append('}');
            return builder.toString();
        }
    }

    public static Pair<String, Map<String, String>> fromFullName(String fullName) {
        int index = fullName.indexOf('{');
        if (index == -1) {
            return new Pair<>(fullName, Collections.emptyMap());
         } else {
            String name = fullName.substring(0, index);
            String[] tagKVs = fullName.substring(index + 1, fullName.length() - 1).split(",");
            Map<String, String> tags = new HashMap<>();
            for (String tagKV: tagKVs) {
                String[] KV = tagKV.split("=", 2);
                tags.put(KV[0], KV[1]);
            }
            return new Pair<>(name, tags);
        }
    }

}
