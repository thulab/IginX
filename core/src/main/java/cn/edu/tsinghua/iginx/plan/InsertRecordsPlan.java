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

import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InsertRecordsPlan extends DataPlan {

	private static final Logger logger = LoggerFactory.getLogger(InsertRecordsPlan.class);

	private long[] timestamps;

	private Object[] values;

	private List<Map<String, Object>> attributes;

	public InsertRecordsPlan(List<String> paths, long[] timestamps, Object[] values,
	    List<Map<String, Object>> attributes) {
		super(IginxPlanType.INSERT_RECORDS, false, paths, timestamps[0], timestamps[timestamps.length - 1]);
		this.timestamps = timestamps;
		this.values = values;
		this.attributes = attributes;
	}

	public InsertRecordsPlan(List<String> paths, long[] timestamps, Object[] values,
	    List<Map<String, Object>> attributes, long databaseId) {
		this(paths, timestamps, values, attributes);
		this.setDatabaseId(databaseId);
	}

	public long[] getTimestamps() {
		return timestamps;
	}

	public long getTimestamp(int index) {
		if (timestamps.length == 0) {
			logger.error("There are no timestamps in the InsertRecordsPlan.");
			return -1L;
		}
		if (index < 0 || index >= timestamps.length) {
			logger.error("The given index {} is out of bounds.", index);
			return -1L;
		}
		return timestamps[index];
	}

	public long[] getTimestampsByRange(long startTime, long endTime) {
		if (timestamps.length == 0) {
			logger.error("There are no timestamps in the InsertRecordsPlan.");
			return null;
		}
		List<Long> tempTimestamps = new ArrayList<>();
		for (int i = 0; i < timestamps.length; i++) {
			if (timestamps[i] >= startTime && timestamps[i] <= endTime) {
				tempTimestamps.add(timestamps[i]);
			}
		}
		return tempTimestamps.stream().mapToLong(i -> i).toArray();
	}

	public Pair<long[], List<Integer>> getTimestampsAndIndexesByRange(long startTime, long endTime) {
		if (timestamps.length == 0) {
			logger.error("There are no timestamps in the InsertRecordsPlan.");
			return null;
		}
		List<Long> tempTimestamps = new ArrayList<>();
		List<Integer> indexes = new ArrayList<>();
		for (int i = 0; i < timestamps.length; i++) {
			if (timestamps[i] >= startTime && timestamps[i] <= endTime) {
				tempTimestamps.add(timestamps[i]);
				indexes.add(i);
			}
		}
		return new Pair<>(tempTimestamps.stream().mapToLong(i -> i).toArray(), indexes);
	}

	public Object[] getValues() {
		return values;
	}

	public Object getValue(int rowIndex, int colIndex) {
		if (values.length == 0) {
			logger.error("There are no values in the InsertRecordsPlan.");
			return null;
		}
		if (rowIndex < 0 || rowIndex >= values.length) {
			logger.error("The given row index {} is out of bounds.", rowIndex);
			return null;
		}
		Object[] colValues = (Object[]) values[rowIndex];
		if (colValues.length == 0) {
			logger.error("There are no col values in the row {}.", rowIndex);
			return null;
		}
		if (colIndex < 0 || colIndex >= colValues.length) {
			logger.error("The given col index {} is out of bounds.", rowIndex);
			return null;
		}
		return colValues[colIndex];
	}

	public Object[] getValuesByIndexes(List<Integer> rowIndexes, List<Integer> colIndexes) {
		if (values.length == 0) {
			logger.error("There are no values in the InsertRecordsPlan.");
			return null;
		}
		Object[] tempValues = new Object[rowIndexes.size()];
		int i = 0;
		for (Integer rowIndex : rowIndexes) {
			Object[] tempColValues = new Object[colIndexes.size()];
			int j = 0;
			for (Integer colIndex : colIndexes) {
				if (getValue(rowIndex, colIndex) != null) {
					tempColValues[j] = getValue(rowIndex, colIndex);
					j++;
				}
			}
			tempValues[i] = tempColValues;
			i++;
		}
		return tempValues;
	}

	public List<Map<String, Object>> getAttributes() {
		return attributes;
	}

	public Map<String, Object> getAttribute(int index) {
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

	public List<Map<String, Object>> getAttributesByIndexes(List<Integer> indexes) {
		if (attributes.isEmpty()) {
			logger.error("There are no attributes in the InsertRecordsPlan.");
			return null;
		}
		List<Map<String, Object>> tempAttributes = new ArrayList<>();
		for (Integer index : indexes) {
			if (getAttribute(index) != null) {
				tempAttributes.add(getAttribute(index));
			}
		}
		return tempAttributes;
	}
}
