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

public class Point {

    private final long timestamp;

    private final Object value;

    private final DataType dataType;

    private final String measurement;

    public Point(long timestamp, Object value, DataType dataType, String measurement) {
        this.timestamp = timestamp;
        this.value = value;
        this.dataType = dataType;
        this.measurement = measurement;
    }

    private Point(Point.Builder builder) {
        this(builder.timestamp, builder.value, builder.dataType, builder.measurement);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Object getValue() {
        return value;
    }

    public DataType getDataType() {
        return dataType;
    }

    public String getMeasurement() {
        return measurement;
    }

    public static Point.Builder builder() {
        return new Point.Builder();
    }

    public static class Builder {

        private long timestamp = -1;

        private Object value;

        private DataType dataType;

        private String measurement;

        private Builder() {

        }

        public Point.Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Point.Builder now() {
            this.timestamp = System.currentTimeMillis();
            return this;
        }

        public Point.Builder dataType(DataType dataType) {
            Arguments.checkNotNull(dataType, "dataType");
            this.dataType = dataType;
            return this;
        }

        public Point.Builder value(Object value) {
            Arguments.checkNotNull(value, "value");
            this.value = value;
            return this;
        }

        public Point.Builder measurement(String measurement) {
            Arguments.checkNonEmpty(measurement, "measurement");
            this.measurement = measurement;
            return this;
        }

        public Point.Builder intValue(int value) {
            this.value = value;
            this.dataType = DataType.INTEGER;
            return this;
        }

        public Point.Builder longValue(long value) {
            this.value = value;
            this.dataType = DataType.LONG;
            return this;
        }

        public Point.Builder floatValue(float value) {
            this.value = value;
            this.dataType = DataType.FLOAT;
            return this;
        }

        public Point.Builder doubleValue(double value) {
            this.value = value;
            this.dataType = DataType.DOUBLE;
            return this;
        }

        public Point.Builder binaryValue(byte[] value) {
            this.value = value;
            this.dataType = DataType.BINARY;
            return this;
        }

        public Point build() {
            Arguments.checkNonEmpty(measurement, "measurement");
            Arguments.checkNotNull(value, "value");
            Arguments.checkNotNull(dataType, "dataType");
            Arguments.checkDataType(value, dataType, "value");
            if (timestamp < 0) {
                timestamp = System.currentTimeMillis();
            }
            return new Point(this);
        }

    }

}
