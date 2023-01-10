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

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public final class Field {

    public static final Field KEY = new Field();

    private final String name;

    private final String fullName;

    private final Map<String, String> tags;

    private final DataType type;

    public Field() {
        this(GlobalConstant.KEY_NAME, DataType.LONG, Collections.emptyMap());
    }

    public Field(String name, DataType type) {
        this(name, type, Collections.emptyMap());
    }

    public Field(String name, DataType type, Map<String, String> tags) {
        this.name = name;
        this.type = type;
        this.tags = tags;
        if (this.tags == null || this.tags.isEmpty()) {
            this.fullName = name;
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
            this.fullName = builder.toString();
        }
    }

    public Field(String name, String fullName, DataType type) {
        this(name, fullName, type, Collections.emptyMap());
    }

    public Field(String name, String fullName, DataType type, Map<String, String> tags) {
        this.name = name;
        this.fullName = fullName;
        this.type = type;
        this.tags = tags;
    }

    public String getName() {
        return name;
    }

    public String getFullName() {
        return fullName;
    }

    public DataType getType() {
        return type;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public String toString() {
        return "Field{" +
                "name='" + name + '\'' +
                ", fullName='" + fullName + '\'' +
                ", type=" + type +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Field that = (Field) o;
        return Objects.equals(name, that.name) && Objects.equals(fullName, that.fullName) && type == that.type && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fullName, type, tags);
    }

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

}
