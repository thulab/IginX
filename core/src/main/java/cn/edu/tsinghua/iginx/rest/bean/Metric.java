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
package cn.edu.tsinghua.iginx.rest.bean;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;

@Data
public class Metric {
    private String name;
    private Long startAbsolute;
    private Long endAbsolute;
    private Map<String, String> tags = new TreeMap<>();
    private List<Long> keys = new ArrayList<>();
    private List<String> values = new ArrayList<>();
    private Map<String, String> anno = new HashMap<>();
    private String annotation = null;

    public void addTag(String key, String value) {
        tags.put(key, value);
    }

    public void addKey(Long key) {
        keys.add(key);
    }

    public void addValue(String value) {
        values.add(value);
    }

    public void addAnno(String key, String value) {
        anno.put(key, value);
    }
}
