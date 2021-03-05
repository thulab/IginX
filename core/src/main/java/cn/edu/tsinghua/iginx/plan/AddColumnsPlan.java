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
import java.util.List;
import java.util.Map;

public class AddColumnsPlan extends ColumnPlan {

	private static final Logger logger = LoggerFactory.getLogger(AddColumnsPlan.class);

	private List<Map<String, String>> attributes;

	public AddColumnsPlan(List<String> paths, List<Map<String, String>> attributes) {
		super(false, paths);
		this.setIginxPlanType(IginxPlanType.ADD_COLUMNS);
		this.attributes = attributes;
	}

	public List<Map<String, String>> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Map<String, String>> attributes) {
		this.attributes = attributes;
	}

	public Map<String, String> getAttribute(int index) {
		if (attributes.isEmpty()) {
			logger.error("There are no attributes in the InsertRecordsPlan.");
			return null;
		}
		if (index < 0 || index >= attributes.size()) {
			logger.error("The given index {} is out of bounds.", index);
			return null;
		}
		return attributes.get(index);
	}

	public List<Map<String, String>> getAttributesByIndexes(List<Integer> indexes) {
		if (attributes.isEmpty()) {
			logger.error("There are no attributes in the InsertRecordsPlan.");
			return null;
		}
		List<Map<String, String>> tempAttributes = new ArrayList<>();
		for (Integer index : indexes) {
			if (getAttribute(index) != null) {
				tempAttributes.add(getAttribute(index));
			}
		}
		return tempAttributes;
	}
}
