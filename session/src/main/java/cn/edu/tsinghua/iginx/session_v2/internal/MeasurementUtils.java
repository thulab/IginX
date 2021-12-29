package cn.edu.tsinghua.iginx.session_v2.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
import java.util.stream.Collectors;

class MeasurementUtils {

    static List<String> mergeAndSortMeasurements(List<String> measurements) {
        if (measurements.stream().anyMatch(x -> x.equals("*"))) {
            List<String> tempPaths = new ArrayList<>();
            tempPaths.add("*");
            return tempPaths;
        }
        List<String> prefixes = measurements.stream().filter(x -> x.contains("*")).map(x -> x.substring(0, x.indexOf("*"))).collect(Collectors.toList());
        if (prefixes.isEmpty()) {
            Collections.sort(measurements);
            return measurements;
        }
        List<String> mergedMeasurements = new ArrayList<>();
        for (String measurement : measurements) {
            if (!measurement.contains("*")) {
                boolean skip = false;
                for (String prefix : prefixes) {
                    if (measurement.startsWith(prefix)) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }
            }
            mergedMeasurements.add(measurement);
        }
        mergedMeasurements.sort(String::compareTo);
        return mergedMeasurements;
    }

}
