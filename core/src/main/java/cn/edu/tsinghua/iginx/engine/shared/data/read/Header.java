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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.*;
import java.util.regex.Pattern;

public final class Header {

    public static final Header EMPTY_HEADER = new Header(Collections.emptyList());

    private final Field key;

    private final List<Field> fields;

    private final Map<String, Integer> indexMap;

    private final Map<String, List<Integer>> patternIndexCache;

    public Header(List<Field> fields) {
        this(null, fields);
    }

    public Header(Field key, List<Field> fields) {
        this.key = key;
        this.fields = fields;
        this.indexMap = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            this.indexMap.put(fields.get(i).getFullName(), i);
        }
        this.patternIndexCache = new HashMap<>();
    }

    public Field getKey() {
        return key;
    }

    public List<Field> getFields() {
        return fields;
    }

    public Field getField(int index) {
        return fields.get(index);
    }

    public int getFieldSize() {
        return fields.size();
    }

    public boolean hasKey() {
        return key != null;
    }

    public int indexOf(Field field) {
        String name = field.getFullName();
        int index = indexMap.getOrDefault(name, -1);
        if (index == -1) {
            return -1;
        }
        Field targetField = fields.get(index);
        if (targetField.equals(field)) {
            return index;
        } else {
            return -1;
        }
    }

    public int indexOf(String name) {
        return indexMap.getOrDefault(name, -1);
    }

    public List<Integer> patternIndexOf(String pattern) {
        if (patternIndexCache.containsKey(pattern)) {
            return patternIndexCache.get(pattern);
        } else {
            List<Integer> indexList = new ArrayList<>();
            fields.forEach(field -> {
                if (Pattern.matches(StringUtils.reformatPath(pattern), field.getFullName())) {
                    indexList.add(indexOf(field.getFullName()));
                }
            });
            patternIndexCache.put(pattern, indexList);
            return indexList;
        }
    }

    @Override
    public String toString() {
        return "Header{" +
            "time=" + key +
            ", fields=" + fields +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Header header = (Header) o;
        return Objects.equals(key, header.key) && Objects.equals(fields, header.fields) && Objects.equals(indexMap, header.indexMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, fields, indexMap);
    }
}
