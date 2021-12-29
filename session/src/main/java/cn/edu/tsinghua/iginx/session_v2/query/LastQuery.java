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
package cn.edu.tsinghua.iginx.session_v2.query;

import cn.edu.tsinghua.iginx.session_v2.Arguments;

import java.util.HashSet;
import java.util.Set;

public class LastQuery extends Query {

    private final long startTime;

    public LastQuery(Set<String> measurements, long startTime) {
        super(measurements);
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public static class Builder {

        private final Set<String> measurements;

        private long startTime;

        private Builder() {
            this.measurements = new HashSet<>();
            this.startTime = 0L;
        }

        public LastQuery.Builder addMeasurement(String measurement) {
            Arguments.checkNonEmpty(measurement, "measurement");
            this.measurements.add(measurement);
            return this;
        }

        public LastQuery.Builder addMeasurements(Set<String> measurements) {
            measurements.forEach(measurement -> Arguments.checkNonEmpty(measurement, "measurement"));
            this.measurements.addAll(measurements);
            return this;
        }

        public LastQuery.Builder startTime(long startTime) {
            if (startTime < 0) {
                throw new IllegalArgumentException("startTime must greater than zero.");
            }
            this.startTime = startTime;
            return this;
        }

        public LastQuery build() {
            if (this.measurements.isEmpty()) {
                throw new IllegalStateException("last query at least has one measurement.");
            }
            return new LastQuery(measurements, startTime);
        }

    }

}
