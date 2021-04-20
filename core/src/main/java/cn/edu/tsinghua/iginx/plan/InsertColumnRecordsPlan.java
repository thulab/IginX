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
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.INSERT_COLUMN_RECORDS;

@ToString
public class InsertColumnRecordsPlan extends InsertRecordsPlan {

	private static final Logger logger = LoggerFactory.getLogger(InsertColumnRecordsPlan.class);

	public InsertColumnRecordsPlan(List<String> paths, long[] timestamps, Object[] valuesList, List<Bitmap> bitmapList,
	                               List<DataType> dataTypeList, List<Map<String, String>> attributesList, long storageEngineId, String storageUnitId) {
		super(paths, timestamps, valuesList, bitmapList, dataTypeList, attributesList, storageEngineId, storageUnitId);
		this.setIginxPlanType(INSERT_COLUMN_RECORDS);
	}

	public InsertColumnRecordsPlan(List<String> paths, long[] timestamps, Object[] valuesList, List<Bitmap> bitmapList,
	                               List<DataType> dataTypeList, List<Map<String, String>> attributesList) {
		this(paths, timestamps, valuesList, bitmapList, dataTypeList, attributesList, -1L, "");
	}

	public Pair<Object[], List<Bitmap>> getValuesAndBitmapsByIndexes(Pair<Integer, Integer> rowIndexes, TimeSeriesInterval interval) {
		if (getValuesList() == null || getValuesList().length == 0) {
			logger.error("There are no values in the InsertColumnRecordsPlan.");
			return null;
		}
		int startIndex;
		int endIndex;
		if (interval.getStartTimeSeries() == null) {
			startIndex = 0;
		} else {
			startIndex = getPathsNum();
			for (int i = 0; i < getPathsNum(); i++) {
				if (getPath(i).compareTo(interval.getStartTimeSeries()) >= 0) {
					startIndex = i;
					break;
				}
			}
		}
		if (interval.getEndTimeSeries() == null) {
			endIndex = getPathsNum() - 1;
		} else {
			endIndex = -1;
			for (int i = getPathsNum() - 1; i >= 0; i--) {
				if (getPath(i).compareTo(interval.getEndTimeSeries()) <= 0) {
					endIndex = i;
					break;
				}
			}
		}

		Object[] tempValues = new Object[endIndex - startIndex + 1];
		List<Bitmap> tempBitmaps = new ArrayList<>();
		for (int i = startIndex; i <= endIndex; i++) {
			Bitmap tempBitmap = new Bitmap(rowIndexes.v - rowIndexes.k + 1);
			List<Integer> indexes = new ArrayList<>();
			int k = 0;
			for (int j = 0; j < getTimestamps().length; j++) {
				if (getBitmap(i).get(j)) {
					if (j >= rowIndexes.k && j <= rowIndexes.v) {
						indexes.add(k);
						tempBitmap.mark(j - rowIndexes.k);
					}
					k++;
				}
			}
			Object[] tempColumnValues = new Object[indexes.size()];
			for (int j = 0; j < indexes.size(); j++) {
				tempColumnValues[j] = getValues(i)[indexes.get(j)];
			}
			tempValues[i - startIndex] = tempColumnValues;
			tempBitmaps.add(tempBitmap);
		}
		return new Pair<>(tempValues, tempBitmaps);
	}
}
