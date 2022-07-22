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

import java.util.*;

public class SimpleQuery extends Query {

    private final long startTime;

    private final long endTime;

    private SimpleQuery(Set<String> measurements, Map<String, List<String>> tagsList, long startTime, long endTime) {
        super(Collections.unmodifiableSet(measurements), tagsList);
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public static SimpleQuery.Builder builder() {
        return new Builder();
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public static class Builder {

        private final Set<String> measurements;

        private final Map<String, List<String>> tagsList;

        private long startTime;

        private long endTime;

        private Builder() {
            this.measurements = new HashSet<>();
            this.tagsList = new HashMap<>();
            this.startTime = 0L;
            this.endTime = Long.MAX_VALUE;
        }

        public SimpleQuery.Builder addMeasurement(String measurement) {
            Arguments.checkNonEmpty(measurement, "measurement");
            this.measurements.add(measurement);
            return this;
        }

        public SimpleQuery.Builder addMeasurements(Set<String> measurements) {
            measurements.forEach(measurement -> Arguments.checkNonEmpty(measurement, "measurement"));
            this.measurements.addAll(measurements);
            return this;
        }

        public SimpleQuery.Builder addTags(String tagK, List<String> valueList) {
            Arguments.checkListNonEmpty(valueList, "valueList");
            this.tagsList.put(tagK, valueList);
            return this;
        }

        public SimpleQuery.Builder addTagsList(Map<String, List<String>> tagsList) {
            tagsList.forEach((key, valueList) -> Arguments.checkListNonEmpty(valueList, "valueList"));
            this.tagsList.putAll(tagsList);
            return this;
        }

        public SimpleQuery.Builder startTime(long startTime) {
            if (startTime < 0) {
                throw new IllegalArgumentException("startTime must greater than zero.");
            }
            if (startTime >= endTime) {
                throw new IllegalArgumentException("startTime must less than endTime.");
            }
            this.startTime = startTime;
            return this;
        }

        public SimpleQuery.Builder endTime(long endTime) {
            if (endTime < 0) {
                throw new IllegalArgumentException("endTime mush greater than zero.");
            }
            if (endTime <= startTime) {
                throw new IllegalArgumentException("endTime must greater than startTime.");
            }
            this.endTime = endTime;
            return this;
        }

        public SimpleQuery build() {
            if (this.measurements.isEmpty()) {
                throw new IllegalStateException("simple query at least has one measurement.");
            }
            return new SimpleQuery(measurements, tagsList, startTime, endTime);
        }

    }

}
