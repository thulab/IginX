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

import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.NON_DATABASE;

public abstract class NonDatabasePlan extends IginxPlan {

    private static final Logger logger = LoggerFactory.getLogger(NonDatabasePlan.class);

    private List<String> paths;

    private TimeSeriesInterval tsInterval;

    protected NonDatabasePlan(boolean isQuery, List<String> paths) {
        super(isQuery);
        this.setIginxPlanType(NON_DATABASE);
        this.setCanBeSplit(true);
        this.paths = paths;
        this.tsInterval = new TimeSeriesInterval(paths.get(0), paths.get(paths.size() - 1));
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public int getPathsNum() {
        return paths.size();
    }

    public String getPath(int index) {
        if (paths.isEmpty()) {
            logger.error("There are no paths in the plan.");
            return null;
        }
        if (index < 0 || index >= paths.size()) {
            logger.error("The given index {} is out of bounds.", index);
            return null;
        }
        return paths.get(index);
    }

    public List<String> getPathsByInterval(TimeSeriesInterval interval) {
        if (paths.isEmpty()) {
            logger.error("There are no paths in the plan.");
            return null;
        }
        return paths.stream().filter(x -> StringUtils.compare(x, interval.getStartTimeSeries(), true) >= 0 && StringUtils.compare(x, interval.getEndTimeSeries(), false) < 0).collect(Collectors.toList());
//        if (interval.getStartTimeSeries() != null && interval.getEndTimeSeries() != null) {
//            // TODO 时间序列区间左闭右开，存在左右端点相等的情况
//            return paths.stream().filter(x -> x.equals(interval.getStartTimeSeries()) || (x.compareTo(interval.getStartTimeSeries()) > 0 && x.compareTo(interval.getEndTimeSeries()) < 0)).collect(Collectors.toList());
//        } else if (interval.getStartTimeSeries() != null) {
//            return paths.stream().filter(x -> x.compareTo(interval.getStartTimeSeries()) >= 0).collect(Collectors.toList());
//        } else if (interval.getEndTimeSeries() != null) {
//            return paths.stream().filter(x -> x.compareTo(interval.getEndTimeSeries()) < 0).collect(Collectors.toList());
//        } else {
//            return paths;
//        }
    }

    public String getStartPath() {
        return tsInterval.getStartTimeSeries();
    }

    public String getEndPath() {
        return tsInterval.getEndTimeSeries();
    }

    public TimeSeriesInterval getTsInterval() {
        return tsInterval;
    }

    public void setTsInterval(TimeSeriesInterval tsInterval) {
        this.tsInterval = tsInterval;
    }
}
