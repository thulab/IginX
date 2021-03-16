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

import cn.edu.tsinghua.iginx.metadatav2.entity.TimeSeriesInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class NonDatabasePlan extends IginxPlan {

	private static final Logger logger = LoggerFactory.getLogger(NonDatabasePlan.class);

	private List<String> paths;

	private TimeSeriesInterval tsInterval;

	protected NonDatabasePlan(boolean isQuery, List<String> paths) {
		super(isQuery);
		this.setIginxPlanType(IginxPlanType.NON_DATABASE);
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
			logger.error("There are no paths in the InsertRecordsPlan.");
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
			logger.error("There are no paths in the InsertRecordsPlan.");
			return null;
		}
		int startIndex = paths.indexOf(interval.getStartTimeSeries());
		int endIndex = interval.getEndTimeSeries() == null ? paths.size() - 1 : paths.indexOf(interval.getEndTimeSeries());
		return paths.subList(startIndex, endIndex + 1);
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
