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
package cn.edu.tsinghua.iginx.plan;
//todo

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.query.expression.BooleanExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.VALUEFILTER_QUERY;

public class ValueFilterQueryPlan extends DataPlan {

    private static final Logger logger = LoggerFactory.getLogger(ValueFilterQueryPlan.class);
    BooleanExpression booleanExpression;

    public ValueFilterQueryPlan(List<String> paths, long startTime, long endTime, BooleanExpression booleanExpression) {
        super(true, paths, startTime, endTime, null);
        this.booleanExpression = booleanExpression;
        this.setIginxPlanType(VALUEFILTER_QUERY);
        paths.addAll(booleanExpression.getTimeseries());
        Collections.sort(paths);
        boolean isStartPrefix = paths.get(0).contains("*");
        String startTimeSeries = trimPath(paths.get(0));
        boolean isEndPrefix = paths.get(getPathsNum() - 1).contains("*");
        String endTimeSeries = trimPath(paths.get(getPathsNum() - 1));
        for (String path : paths) {
            boolean isPrefix = path.contains("*");
            String prefix = trimPath(path);
            if (startTimeSeries.compareTo(prefix) >= 0) {
                startTimeSeries = prefix;
                isStartPrefix = isPrefix;
            }
            if (endTimeSeries.compareTo(prefix) <= 0) {
                endTimeSeries = prefix;
                isEndPrefix = isPrefix;
            }
        }
        if (isStartPrefix) {
            startTimeSeries = addEndPrefix(startTimeSeries, true);
        }
        if (isEndPrefix) {
            endTimeSeries = addEndPrefix(endTimeSeries, false);
        }
        this.setTsInterval(new TimeSeriesInterval(startTimeSeries, endTimeSeries));
    }

    public ValueFilterQueryPlan(List<String> paths, long startTime, long endTime, BooleanExpression booleanExpression, StorageUnitMeta storageUnit) {
        this(paths, startTime, endTime, booleanExpression);
        this.setStorageUnit(storageUnit);
        this.setSync(true);
    }

    private static String trimPath(String path) {
        int index = path.indexOf("*");
        if (index == -1) { // 不含有 *，则不对字符串进行变更
            return path;
        }
        if (index == 0) {
            return "";
        }
        return path.substring(0, index - 1);
    }

    private static String addEndPrefix(String path, boolean start) {
        if (path.length() != 0) {
            path += ".";
        }
        if (start) {
            path += (char) ('A' - 1);
        } else {
            path += (char) ('z' + 1);
        }
        return path;
    }

    public List<String> getPathsByInterval(TimeSeriesInterval interval) {
        List<String> paths = getPaths();
        paths.addAll(booleanExpression.getTimeseries());
        Collections.sort(paths);
        if (paths.isEmpty()) {
            logger.error("There are no paths in the plan.");
            return null;
        }
        if (interval.getStartTimeSeries() == null && interval.getEndTimeSeries() == null) {
            return paths;
        }
        List<String> tempPaths = new ArrayList<>();
        for (String path : paths) {
            String prefix = trimPath(path).contains("*") ? path.substring(0, path.indexOf("*") - 1) : trimPath(path);
            if (interval.getStartTimeSeries() != null && prefix.compareTo(interval.getStartTimeSeries()) < 0 && !interval.getStartTimeSeries().startsWith(prefix)) {
                continue;
            }
            if (interval.getEndTimeSeries() != null && prefix.compareTo(interval.getEndTimeSeries()) > 0) {
                continue;
            }
            tempPaths.add(path);
        }
        return tempPaths;
    }

    public BooleanExpression getBooleanExpression() {
        return booleanExpression;
    }
}
