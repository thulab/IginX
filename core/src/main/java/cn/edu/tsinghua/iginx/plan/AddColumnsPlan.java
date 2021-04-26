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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.ADD_COLUMNS;

public class AddColumnsPlan extends ColumnPlan {

	private static final Logger logger = LoggerFactory.getLogger(AddColumnsPlan.class);

	private List<Map<String, String>> attributesList;

	public AddColumnsPlan(List<String> paths, List<Map<String, String>> attributesList) {
		super(false, paths);
		this.setIginxPlanType(ADD_COLUMNS);
		this.attributesList = attributesList;
	}

	public List<Map<String, String>> getAttributesList() {
		return attributesList;
	}

	public void setAttributesList(List<Map<String, String>> attributesList) {
		this.attributesList = attributesList;
	}

	public Map<String, String> getAttributes(int index) {
		if (attributesList.isEmpty()) {
			logger.error("There are no attributes in the InsertRecordsPlan.");
			return null;
		}
		if (index < 0 || index >= attributesList.size()) {
			logger.error("The given index {} is out of bounds.", index);
			return null;
		}
		return attributesList.get(index);
	}

	public List<Map<String, String>> getAttributesByInterval(TimeSeriesInterval interval) {
		if (attributesList.isEmpty()) {
			logger.error("There are no attributes in the InsertRecordsPlan.");
			return null;
		}
		int startIndex = interval.getStartTimeSeries() == null ? 0 : getPathsNum();
		int endIndex = interval.getEndTimeSeries() == null ? getPathsNum() - 1 : -1;
		for (int i = 0; i < getPathsNum(); i++) {
			if (interval.getStartTimeSeries() != null && getPath(i).compareTo(interval.getStartTimeSeries()) >= 0 && i < startIndex) {
				startIndex = i;
			}
			if (interval.getEndTimeSeries() != null && getPath(i).compareTo(interval.getEndTimeSeries()) < 0 && i > endIndex) {
				endIndex = i;
			}
		}
		return attributesList.subList(startIndex, endIndex + 1);
	}
}
