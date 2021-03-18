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

import cn.edu.tsinghua.iginx.metadatav2.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import lombok.ToString;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@ToString
public class InsertRecordsPlan extends DataPlan {

	private static final Logger logger = LoggerFactory.getLogger(InsertRecordsPlan.class);

	private long[] timestamps;

	private Object[] valuesList;

	private List<DataType> dataTypeList;

	private List<Map<String, String>> attributesList;

	public InsertRecordsPlan(List<String> paths, long[] timestamps, Object[] valuesList,
	        List<DataType> dataTypeList, List<Map<String, String>> attributesList) {
		super(false, paths, timestamps[0], timestamps[timestamps.length - 1]);
		this.setIginxPlanType(IginxPlanType.INSERT_RECORDS);
		this.timestamps = timestamps;
		this.valuesList = valuesList;
		this.dataTypeList = dataTypeList;
		this.attributesList = attributesList;
	}

	public InsertRecordsPlan(List<String> paths, long[] timestamps, Object[] valuesList,
	        List<DataType> dataTypeList, List<Map<String, String>> attributesList, long databaseId) {
		this(paths, timestamps, valuesList, dataTypeList, attributesList);
		this.setStorageEngineId(databaseId);
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
		for (long timestamp : timestamps) {
			if (timestamp >= startTime && timestamp <= endTime) {
				tempTimestamps.add(timestamp);
			}
		}
		return tempTimestamps.stream().mapToLong(i -> i).toArray();
	}

	public Pair<long[], Pair<Integer, Integer>> getTimestampsAndIndexesByInterval(TimeInterval interval) {
		if (timestamps.length == 0) {
			logger.error("There are no timestamps in the InsertRecordsPlan.");
			return null;
		}
		List<Long> tempTimestamps = new ArrayList<>();
		int startIndex = timestamps.length;
		int endIndex = 0;
		for (int i = 0; i < timestamps.length; i++) {
			if (timestamps[i] >= interval.getStartTime() && timestamps[i] <= interval.getEndTime()) {
				tempTimestamps.add(timestamps[i]);
				startIndex = Math.min(startIndex, i);
				endIndex = Math.max(endIndex, i);
			}
		}
		return new Pair<>(tempTimestamps.stream().mapToLong(Long::longValue).toArray(), new Pair<>(startIndex, endIndex));
	}

	public Object[] getValuesList() {
		return valuesList;
	}

	public Object[] getValuesByIndexes(Pair<Integer, Integer> rowIndexes, TimeSeriesInterval interval) {
		if (valuesList == null || valuesList.length == 0) {
			logger.error("There are no values in the InsertRecordsPlan.");
			return null;
		}
		int startIndex = getPaths().indexOf(interval.getStartTimeSeries());
		int endIndex = interval.getEndTimeSeries() == null ? getPathsNum() - 1 : getPaths().indexOf(interval.getEndTimeSeries());;
		Object[] tempValues = new Object[endIndex - startIndex + 1];
		for (int i = startIndex; i <= endIndex; i++) {
			Object[] tempColValues;
			switch (getDataType(i)) {
				case BOOLEAN:
					tempColValues = ArrayUtils.toObject((boolean[]) valuesList[i]);
					break;
				case INTEGER:
					tempColValues = ArrayUtils.toObject((int[]) valuesList[i]);
					break;
				case LONG:
					tempColValues = ArrayUtils.toObject((long[]) valuesList[i]);
					break;
				case FLOAT:
					tempColValues = ArrayUtils.toObject((float[]) valuesList[i]);
					break;
				case DOUBLE:
					tempColValues = ArrayUtils.toObject((double[]) valuesList[i]);
					break;
				case STRING:
					// TODO
					tempColValues = (String[]) valuesList[i];
					break;
				default:
					throw new UnsupportedOperationException(getDataType(i).toString());
			}
			tempValues[i - startIndex] = Arrays.copyOfRange(tempColValues, rowIndexes.k, rowIndexes.v + 1);
		}
		return tempValues;
	}

	public List<DataType> getDataTypeList() {
		return dataTypeList;
	}

	public DataType getDataType(int index) {
		if (dataTypeList == null || dataTypeList.isEmpty()) {
			logger.error("There are no DataType in the InsertRecordsPlan.");
			return null;
		}
		if (index < 0 || index >= dataTypeList.size()) {
			logger.error("The given index {} is out of bounds.", index);
			return null;
		}
		return dataTypeList.get(index);
	}

	public List<DataType> getDataTypeListByInterval(TimeSeriesInterval interval) {
		if (dataTypeList == null || dataTypeList.isEmpty()) {
			logger.error("There are no DataType in the InsertRecordsPlan.");
			return null;
		}
		int startIndex = getPaths().indexOf(interval.getStartTimeSeries());
		int endIndex = interval.getEndTimeSeries() == null ? getPathsNum() - 1 : getPaths().indexOf(interval.getEndTimeSeries());
		return dataTypeList.subList(startIndex, endIndex + 1);
	}

	public List<Map<String, String>> getAttributesList() {
		return attributesList;
	}

	public Map<String, String> getAttributes(int index) {
		if (attributesList == null || attributesList.isEmpty()) {
			logger.info("There are no attributes in the InsertRecordsPlan.");
			return null;
		}
		if (index < 0 || index >= attributesList.size()) {
			logger.error("The given index {} is out of bounds.", index);
			return null;
		}
		return attributesList.get(index);
	}

	public List<Map<String, String>> getAttributesByInterval(TimeSeriesInterval interval) {
		if (attributesList == null || attributesList.isEmpty()) {
			logger.info("There are no attributes in the InsertRecordsPlan.");
			return null;
		}
		int startIndex = getPaths().indexOf(interval.getStartTimeSeries());
		int endIndex = interval.getEndTimeSeries() == null ? getPathsNum() - 1 : getPaths().indexOf(interval.getEndTimeSeries());
		return attributesList.subList(startIndex, endIndex + 1);
	}
}
