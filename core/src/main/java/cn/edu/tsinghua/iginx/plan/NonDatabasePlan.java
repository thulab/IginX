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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.utils.SplitUtils.getKeyFromPath;

public abstract class NonDatabasePlan extends IginxPlan {

	private static final Logger logger = LoggerFactory.getLogger(NonDatabasePlan.class);

	private List<String> paths;

	public NonDatabasePlan(boolean isQuery, List<String> paths) {
		super(isQuery);
		this.setIginxPlanType(IginxPlanType.NON_DATABASE);
		this.setCanBeSplit(true);
		this.paths = paths;
	}

	public Map<String, List<Integer>> generateIndexesOfPaths() {
		Map<String, List<Integer>> indexesOfPaths = new HashMap<>();

		for (int i = 0; i < getPathsNum(); i++) {
			String key = getKeyFromPath(getPath(i));
			if (!indexesOfPaths.containsKey(key)) {
				List<Integer> indexes = new ArrayList<>();
				indexes.add(i);
				indexesOfPaths.put(key, indexes);
			} else {
				indexesOfPaths.get(key).add(i);
			}
		}
		return indexesOfPaths;
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

	public List<String> getPathsByIndexes(List<Integer> indexes) {
		if (paths.isEmpty()) {
			logger.error("There are no paths in the InsertRecordsPlan.");
			return null;
		}
		List<String> tempPaths = new ArrayList<>();
		for (Integer index : indexes) {
			if (getPath(index) != null) {
				tempPaths.add(getPath(index));
			}
		}
		return tempPaths;
	}
}
