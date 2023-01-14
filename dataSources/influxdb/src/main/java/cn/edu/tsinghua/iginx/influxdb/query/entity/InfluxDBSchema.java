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
package cn.edu.tsinghua.iginx.influxdb.query.entity;

import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InfluxDBSchema {

    public static final String TAG = "t";

    private final String measurement;

    private final String field;

    private final Map<String, String> tags;

    public InfluxDBSchema(String path, Map<String, String> tags) {
        int index = path.indexOf(".");
        this.measurement = path.substring(0, index);
        this.field = path.substring(index + 1);

        if (tags == null) {
            this.tags = Collections.emptyMap();
        } else {
            this.tags = tags;
        }
    }

    public InfluxDBSchema(String path) {
        this(path, null);
    }

    public String getMeasurement() {
        return measurement;
    }

    public String getField() {
        return field;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public static String transformField(String field) {
        String[] parts = field.split("\\.");
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i != 0) {
                builder.append(".");
            }
            if (parts[i].equals("*")) {
                builder.append(".+");
            } else {
                builder.append(parts[i]);
            }
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return "InfluxDBSchema{" +
                "measurement='" + measurement + '\'' +
                ", field='" + field + '\'' +
                ", tags=" + tags +
                '}';
    }
}
