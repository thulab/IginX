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
package cn.edu.tsinghua.iginx.session_v2.write;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Record {

    private final long timestamp;

    private final List<String> measurements;

    private final List<DataType> dataTypes;

    private final List<Object> values;

    private Record(long timestamp, List<String> measurements, List<DataType> dataTypes, List<Object> values) {
        this.timestamp = timestamp;
        this.measurements = Collections.unmodifiableList(measurements);
        this.dataTypes = Collections.unmodifiableList(dataTypes);
        this.values = Collections.unmodifiableList(values);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<String> getMeasurements() {
        return measurements;
    }

    public String getMeasurement(int index) {
        return measurements.get(index);
    }

    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    public DataType getDataType(int index) {
        return dataTypes.get(index);
    }

    public List<Object> getValues() {
        return values;
    }

    public Object getValue(int index) {
        return values.get(index);
    }

    public int getLength() {
        return measurements.size();
    }

    public static Record.Builder builder() {
        return new Record.Builder();
    }

    public static class Builder {

        private long timestamp;

        private String measurement;

        private final Map<String, Integer> fieldIndexMap;

        private final List<Object> values;

        private final List<DataType> dataTypes;

        private final List<String> fields;

        private Builder() {
            this.timestamp = -1;
            this.measurement = null;
            this.fields = new ArrayList<>();
            this.values = new ArrayList<>();
            this.dataTypes = new ArrayList<>();
            this.fieldIndexMap = new HashMap<>();
        }

        public Record.Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Record.Builder now() {
            this.timestamp = System.currentTimeMillis();
            return this;
        }

        public Record.Builder measurement(String measurement) {
            Arguments.checkNotNull(measurement, "measurement");
            this.measurement = measurement;
            return this;
        }

        public Record.Builder addIntField(String field, Integer value) {
            Arguments.checkNotNull(field, "field");
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                this.fieldIndexMap.put(field, this.fields.size());
                this.fields.add(field);
                this.values.add(value);
                this.dataTypes.add(DataType.INTEGER);
            } else {
                this.values.set(index, value);
                this.dataTypes.set(index, DataType.INTEGER);
            }
            return this;
        }

        public Record.Builder addLongField(String field, Long value) {
            Arguments.checkNotNull(field, "field");
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                this.fieldIndexMap.put(field, this.fields.size());
                this.fields.add(field);
                this.values.add(value);
                this.dataTypes.add(DataType.LONG);
            } else {
                this.values.set(index, value);
                this.dataTypes.set(index, DataType.LONG);
            }
            return this;
        }

        public Record.Builder addFloatField(String field, Float value) {
            Arguments.checkNotNull(field, "field");
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                this.fieldIndexMap.put(field, this.fields.size());
                this.fields.add(field);
                this.values.add(value);
                this.dataTypes.add(DataType.FLOAT);
            } else {
                this.values.set(index, value);
                this.dataTypes.set(index, DataType.FLOAT);
            }
            return this;
        }

        public Record.Builder addDoubleField(String field, Double value) {
            Arguments.checkNotNull(field, "field");
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                this.fieldIndexMap.put(field, this.fields.size());
                this.fields.add(field);
                this.values.add(value);
                this.dataTypes.add(DataType.DOUBLE);
            } else {
                this.values.set(index, value);
                this.dataTypes.set(index, DataType.DOUBLE);
            }
            return this;
        }

        public Record.Builder addBooleanField(String field, Boolean value) {
            Arguments.checkNotNull(field, "field");
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                this.fieldIndexMap.put(field, this.fields.size());
                this.fields.add(field);
                this.values.add(value);
                this.dataTypes.add(DataType.BOOLEAN);
            } else {
                this.values.set(index, value);
                this.dataTypes.set(index, DataType.BOOLEAN);
            }
            return this;
        }

        public Record.Builder addBinaryField(String field, byte[] value) {
            Arguments.checkNotNull(field, "field");
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                this.fieldIndexMap.put(field, this.fields.size());
                this.fields.add(field);
                this.values.add(value);
                this.dataTypes.add(DataType.BINARY);
            } else {
                this.values.set(index, value);
                this.dataTypes.set(index, DataType.BINARY);
            }
            return this;
        }

        public Record build() {
            if (timestamp < 0) {
                timestamp = System.currentTimeMillis();
            }
            List<String> measurements = fields;
            if (measurement != null) {
                measurements = fields.stream().map(e -> measurement + "." + e).collect(Collectors.toList());
            }
            return new Record(timestamp, measurements, dataTypes, values);
        }

    }

}
