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

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.QUERY_DATA;

public class QueryDataPlan extends DataPlan {

	private static final Logger logger = LoggerFactory.getLogger(QueryDataPlan.class);

	public QueryDataPlan(List<String> paths, long startTime, long endTime, StorageUnitMeta storageUnit) {
		super(true, paths, startTime, endTime, storageUnit);
		this.setIginxPlanType(QUERY_DATA);
		boolean isStartPrefix = paths.get(0).contains("*");
		String startTimeSeries = isStartPrefix ?
				paths.get(0).substring(0, paths.get(0).indexOf("*") - 1) : paths.get(0);
		boolean isEndPrefix = paths.get(getPathsNum() - 1).contains("*");
		String endTimeSeries = isEndPrefix ?
				paths.get(getPathsNum() - 1).substring(0, paths.get(getPathsNum() - 1).indexOf("*") - 1) : paths.get(getPathsNum() - 1);
		for (String path : paths) {
			boolean isPrefix = path.contains("*");
			String prefix = isPrefix ? path.substring(0, path.indexOf("*") - 1) : path;
			if (startTimeSeries.compareTo(prefix) >= 0) {
				startTimeSeries = prefix;
				isStartPrefix = true;
			}
			if (endTimeSeries.compareTo(prefix) <= 0) {
				endTimeSeries = prefix;
				isEndPrefix = true;
			}
		}
		if (isStartPrefix) {
			startTimeSeries += "." +  (char)('A' - 1);
		}
		if (isEndPrefix) {
			endTimeSeries += "." +  (char)('z' + 1);
		}
		this.setTsInterval(new TimeSeriesInterval(startTimeSeries, endTimeSeries));
		this.setSync(true);
	}

	public QueryDataPlan(List<String> paths, long startTime, long endTime) {
		this(paths, startTime, endTime, null);
	}

	public List<String> getPathsByInterval(TimeSeriesInterval interval) {
		if (getPaths().isEmpty()) {
			logger.error("There are no paths in the plan.");
			return null;
		}
		if (interval.getStartTimeSeries() == null && interval.getEndTimeSeries() == null) {
			return getPaths();
		}
		List<String> tempPaths = new ArrayList<>();
		for (String path : getPaths()) {
			String prefix = path.contains("*") ? path.substring(0, path.indexOf("*") - 1) : path;
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
}
