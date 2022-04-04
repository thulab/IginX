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

import java.util.HashMap;
import java.util.Map;

public class InfluxDBSchema {

    public static final String TAG = "t";

    private final String measurement;

    private final String field;

    private final Map<String, String> tags;

    public InfluxDBSchema(String path) {
        int firstIndex = path.indexOf(".");
        this.measurement = path.substring(0, firstIndex);
        int lastIndex = path.lastIndexOf(".");
        this.field = path.substring(lastIndex + 1);

        this.tags = new HashMap<>();

        if (firstIndex != lastIndex) {
            this.tags.put(TAG, path.substring(firstIndex + 1, lastIndex));
        }
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

    @Override
    public String toString() {
        return "InfluxDBSchema{" +
                "measurement='" + measurement + '\'' +
                ", field='" + field + '\'' +
                ", tags=" + tags +
                '}';
    }
}
