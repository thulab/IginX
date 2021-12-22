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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class Table {

    private final List<Long> timestamps;

    private final List<String> measurements;

    private final List<DataType> dataTypes;

    private final List<Object[]> valuesList;

    private Table(List<Long> timestamps, List<String> measurements, List<DataType> dataTypes, List<Object[]> valuesList) {
        this.timestamps = timestamps;
        this.measurements = measurements;
        this.dataTypes = dataTypes;
        this.valuesList = valuesList;
    }

    public static Table.Builder builder() {
        return new Table.Builder();
    }

    public List<Long> getTimestamps() {
        return timestamps;
    }

    public List<String> getMeasurements() {
        return measurements;
    }

    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    public List<Object[]> getValuesList() {
        return valuesList;
    }

    public int getLength() {
        return timestamps.size();
    }

    public long getTimestamp(int index) {
        return timestamps.get(index);
    }

    public Object[] getValues(int index) {
        return valuesList.get(index);
    }

    public String getMeasurement(int index) {
        return measurements.get(index);
    }

    public DataType getDataType(int index) {
        return dataTypes.get(index);
    }

    public static class Builder {

        private final SortedMap<String, Integer> fieldIndexMap;
        private final List<DataType> dataTypes;
        private final List<Long> timestamps;
        private final List<Map<Integer, Object>> valuesList;
        private String measurement;
        private long currentTimestamp;

        private Map<Integer, Object> currentValues;

        private Builder() {
            this.measurement = null;
            this.fieldIndexMap = new TreeMap<>();
            this.dataTypes = new ArrayList<>();
            this.timestamps = new ArrayList<>();
            this.valuesList = new ArrayList<>();

            this.currentTimestamp = -1;
            this.currentValues = new HashMap<>();
        }

        public Table.Builder measurement(String measurement) {
            Arguments.checkNonEmpty(measurement, "measurement");
            this.measurement = measurement;
            return this;
        }

        public Table.Builder addField(String field, DataType dataType) {
            Arguments.checkNotNull(field, "field");
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                index = fieldIndexMap.size();
                this.fieldIndexMap.put(field, index);
                this.dataTypes.add(dataType);
            } else {
                if (dataType != this.dataTypes.get(index)) {
                    throw new IllegalStateException("field " + field + " has add to table, but has different dataType");
                }
            }
            return this;
        }

        public Table.Builder timestamp(long timestamp) {
            this.currentTimestamp = timestamp;
            return this;
        }

        public Table.Builder next() {
            if (currentTimestamp == -1) {
                throw new IllegalStateException("timestamp for current row hasn't set.");
            }
            if (currentValues.isEmpty()) {
                throw new IllegalStateException("current row is empty.");
            }
            this.timestamps.add(currentTimestamp);
            this.valuesList.add(currentValues);
            this.currentTimestamp = -1;
            this.currentValues = new HashMap<>();
            return this;
        }

        public Table.Builder intValue(String field, int value) {
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                throw new IllegalArgumentException("unknown field " + field);
            }
            if (this.dataTypes.get(index) != DataType.INTEGER) {
                throw new IllegalArgumentException("field " + field + " is not integer.");
            }
            this.currentValues.put(index, value);
            return this;
        }

        public Table.Builder longValue(String field, long value) {
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                throw new IllegalArgumentException("unknown field " + field);
            }
            if (this.dataTypes.get(index) != DataType.LONG) {
                throw new IllegalArgumentException("field " + field + " is not long.");
            }
            this.currentValues.put(index, value);
            return this;
        }

        public Table.Builder floatValue(String field, float value) {
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                throw new IllegalArgumentException("unknown field " + field);
            }
            if (this.dataTypes.get(index) != DataType.FLOAT) {
                throw new IllegalArgumentException("field " + field + " is not float.");
            }
            this.currentValues.put(index, value);
            return this;
        }

        public Table.Builder doubleValue(String field, double value) {
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                throw new IllegalArgumentException("unknown field " + field);
            }
            if (this.dataTypes.get(index) != DataType.DOUBLE) {
                throw new IllegalArgumentException("field " + field + " is not double.");
            }
            this.currentValues.put(index, value);
            return this;
        }

        public Table.Builder binaryValue(String field, byte[] value) {
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                throw new IllegalArgumentException("unknown field " + field);
            }
            if (this.dataTypes.get(index) != DataType.BINARY) {
                throw new IllegalArgumentException("field " + field + " is not binary.");
            }
            this.currentValues.put(index, value);
            return this;
        }

        public Table.Builder value(String field, Object value) {
            int index = fieldIndexMap.getOrDefault(field, -1);
            if (index == -1) {
                throw new IllegalArgumentException("unknown field " + field);
            }
            DataType dataType = this.dataTypes.get(index);
            Arguments.checkDataType(value, dataType, field);
            this.currentValues.put(index, value);
            return this;
        }

        public Table build() {
            if (currentTimestamp != -1 && !currentValues.isEmpty()) {
                this.timestamps.add(currentTimestamp);
                this.valuesList.add(currentValues);
            }
            List<String> measurements = new ArrayList<>(fieldIndexMap.keySet());
            if (measurement != null) {
                measurements = measurements.stream().map(e -> measurement + "." + e).collect(Collectors.toList());
            }
            List<Object[]> valuesList = new ArrayList<>();
            for (Map<Integer, Object> rowMap : this.valuesList) {
                Object[] values = new Object[measurements.size()];
                for (Map.Entry<Integer, Object> entry : rowMap.entrySet()) {
                    values[entry.getKey()] = entry.getValue();
                }
                valuesList.add(values);
            }
            return new Table(this.timestamps, measurements, this.dataTypes, valuesList);
        }

    }

}
