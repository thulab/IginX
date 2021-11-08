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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Header {

    private final Field time;

    private final List<Field> fields;

    private final Map<String, Integer> indexMap;

    public Header(List<Field> fields) {
        this(null, fields);
    }

    public Header(Field time, List<Field> fields) {
        this.time = time;
        this.fields = fields;
        this.indexMap = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            this.indexMap.put(fields.get(i).getName(), i);
        }
    }

    public Field getTime() {
        return time;
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

    public boolean hasTimestamp() {
        return time != null;
    }

    public int indexOf(Field field) {
        String name = field.getName();
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

    @Override
    public String toString() {
        return "Header{" +
                "time=" + time +
                ", fields=" + fields +
                '}';
    }
}
