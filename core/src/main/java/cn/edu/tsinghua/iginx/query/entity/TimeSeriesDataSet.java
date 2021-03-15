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
package cn.edu.tsinghua.iginx.query.entity;

import cn.edu.tsinghua.iginx.thrift.DataType;
import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class TimeSeriesDataSet {

    private String name;

    private List<Long> timestamps;

    private List<Object> values;

    private DataType type;

    public TimeSeriesDataSet(String name, DataType dataType) {
        this.name = name;
        this.type = dataType;
        this.timestamps = new LinkedList<>();
        this.values = new LinkedList<>();
    }

    public void addDataPoint(long timestamp, Object value) {
        this.timestamps.add(timestamp);
        this.values.add(value);
    }

    public int length() {
        return timestamps.size();
    }

    public long getTime(int i) {
        return timestamps.get(i);
    }

    public Object getValue(int i) {
        return values.get(i);
    }

    public long getStartTime() {
        return timestamps.get(0);
    }

}
