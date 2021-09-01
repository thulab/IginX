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
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.INSERT_ROW_RECORDS;

@ToString
public class InsertRowRecordsPlan extends InsertNonAlignedRowRecordsPlan {

    private static final Logger logger = LoggerFactory.getLogger(InsertRowRecordsPlan.class);

    public InsertRowRecordsPlan(List<String> paths, long[] timestamps, Object[] valuesList,
                                List<DataType> dataTypeList, List<Map<String, String>> attributesList, StorageUnitMeta storageUnit) {
        super(paths, timestamps, valuesList, null, dataTypeList, attributesList, storageUnit);
        this.setIginxPlanType(INSERT_ROW_RECORDS);
    }

    public InsertRowRecordsPlan(List<String> paths, long[] timestamps, Object[] valuesList,
                                List<DataType> dataTypeList, List<Map<String, String>> attributesList) {
        this(paths, timestamps, valuesList, dataTypeList, attributesList, null);
    }

    public Object[] getValuesByIndexes(Pair<Integer, Integer> rowIndexes, TimeSeriesInterval interval) {
        if (getValuesList() == null || getValuesList().length == 0) {
            logger.error("There are no values in the InsertNonAlignedRowRecordsPlan.");
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

        Object[] tempValues = new Object[rowIndexes.v - rowIndexes.k + 1];
        for (int i = rowIndexes.k; i <= rowIndexes.v; i++) {
            Object[] tempRowValues = new Object[endIndex - startIndex + 1];
            for (int j = startIndex; j <= startIndex; j++) {
                tempRowValues[j - startIndex] = getValues(i)[j];
            }
            tempValues[i - rowIndexes.k] = tempRowValues;
        }
        return tempValues;
    }

}
