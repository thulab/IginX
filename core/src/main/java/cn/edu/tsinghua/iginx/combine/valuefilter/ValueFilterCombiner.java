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
package cn.edu.tsinghua.iginx.combine.valuefilter;
//todo

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.query.expression.BooleanExpression;
import cn.edu.tsinghua.iginx.query.result.ValueFilterQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryResp;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ValueFilterCombiner {

    private static final Logger logger = LoggerFactory.getLogger(ValueFilterCombiner.class);

    private static final ValueFilterCombiner instance = new ValueFilterCombiner();

    private ValueFilterCombiner() {
    }

    public static ValueFilterCombiner getInstance() {
        return instance;
    }

    public boolean insPath(String path, List<String> queryPaths) {
        for (String queryPath : queryPaths) {
            if (path.compareTo(queryPath) == 0)
                return true;
            String[] tmpPath = path.split("\\.");
            String[] tmpQueryPath = queryPath.split("\\.");
            if (tmpPath.length != tmpQueryPath.length) continue;
            boolean flag = true;
            for (int i = 0; i < tmpPath.length; i++)
                if (tmpPath[i].compareTo(tmpQueryPath[i]) != 0 && tmpQueryPath[i].compareTo("*") != 0) {
                    flag = false;
                    break;
                }
            if (flag) return true;
        }
        return false;
    }

    public void combineResult(ValueFilterQueryResp resp, List<ValueFilterQueryPlanExecuteResult> planExecuteResults, List<String> queryPaths, BooleanExpression booleanExpression) throws ExecutionException {
        Set<QueryExecuteDataSetWrapper> dataSetWrappers = planExecuteResults.stream().filter(e -> e.getQueryExecuteDataSets() != null)
                .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode())
                .map(ValueFilterQueryPlanExecuteResult::getQueryExecuteDataSets)
                .flatMap(Collection::stream)
                .map(CheckedFunction.wrap(QueryExecuteDataSetWrapper::new))
                .collect(Collectors.toSet());

        List<String> columnNameList = new ArrayList<>();
        List<DataType> columnTypeList = new ArrayList<>();
        List<String> newColumnNameList = new ArrayList<>();
        List<DataType> newColumnTypeList = new ArrayList<>();
        Map<String, List<QueryExecuteDataSetWrapper>> columnSourcesList = new HashMap<>();
        List<Long> timestamps = new ArrayList<>();
        List<ByteBuffer> valuesList = new ArrayList<>();
        List<ByteBuffer> bitmapList = new ArrayList<>();
        Map<String, Integer> columnPositionMap = new HashMap<>();
        for (QueryExecuteDataSetWrapper dataSetWrapper : dataSetWrappers) {
            List<String> columnNameSubList = dataSetWrapper.getColumnNames();
            List<DataType> columnTypeSubList = dataSetWrapper.getColumnTypes();
            for (int i = 0; i < columnNameSubList.size(); i++) {
                String columnName = columnNameSubList.get(i);
                DataType columnType = columnTypeSubList.get(i);
                if (!columnPositionMap.containsKey(columnName)) {
                    columnPositionMap.put(columnName, columnNameList.size());
                    columnNameList.add(columnName);
                    columnTypeList.add(columnType);
                    columnSourcesList.put(columnName, new ArrayList<>());
                    if (insPath(columnName, queryPaths)) {
                        newColumnNameList.add(columnName);
                        newColumnTypeList.add(columnType);
                    }
                }
                columnSourcesList.get(columnName).add(dataSetWrapper);
            }
        }
        {
            Iterator<QueryExecuteDataSetWrapper> it = dataSetWrappers.iterator();
            Set<QueryExecuteDataSetWrapper> deletedDataSetWrappers = new HashSet<>();
            while (it.hasNext()) {
                QueryExecuteDataSetWrapper dataSetWrapper = it.next();
                if (dataSetWrapper.hasNext()) {
                    dataSetWrapper.next();
                } else {
                    dataSetWrapper.close();
                    deletedDataSetWrappers.add(dataSetWrapper);
                    it.remove();
                }
            }
            for (QueryExecuteDataSetWrapper dataSetWrapper : deletedDataSetWrappers) {
                List<String> columnNames = dataSetWrapper.getColumnNames();
                for (String columnName : columnNames) {
                    columnSourcesList.get(columnName).remove(dataSetWrapper);
                }
            }
        }

        while (!dataSetWrappers.isEmpty()) {
            long timestamp = Long.MAX_VALUE;
            for (QueryExecuteDataSetWrapper dataSetWrapper : dataSetWrappers) {
                timestamp = Math.min(dataSetWrapper.getTimestamp(), timestamp);
            }
            Map<String, Object> objectMap = new HashMap<>();
            Object[] values = new Object[newColumnTypeList.size()];
            Bitmap bitmap = new Bitmap(newColumnTypeList.size());
            for (int i = 0; i < newColumnTypeList.size(); i++) {
                String columnName = newColumnNameList.get(i);
                List<QueryExecuteDataSetWrapper> columnSources = columnSourcesList.get(columnName);
                if (columnSources == null) {
                    continue;
                }
                for (QueryExecuteDataSetWrapper dataSetWrapper : columnSources) {
                    if (dataSetWrapper.getTimestamp() == timestamp) {
                        Object value = dataSetWrapper.getValue(columnName);
                        if (value != null) {
                            values[i] = value;
                            bitmap.mark(i);
                            break;
                        }
                    }
                }
            }

            for (int i = 0; i < booleanExpression.getTimeseries().size(); i++) {
                String columnName = booleanExpression.getTimeseries().get(i);
                List<QueryExecuteDataSetWrapper> columnSources = columnSourcesList.get(columnName);
                if (columnSources == null) {
                    continue;
                }
                for (QueryExecuteDataSetWrapper dataSetWrapper : columnSources) {
                    if (dataSetWrapper.getTimestamp() == timestamp) {
                        Object value = dataSetWrapper.getValue(columnName);
                        if (value != null) {
                            objectMap.put(columnName, value);
                            break;
                        }
                    }
                }
            }

            if (booleanExpression.getBool(objectMap)) {
                timestamps.add(timestamp);
                ByteBuffer buffer = ByteUtils.getRowByteBuffer(values, newColumnTypeList);
                valuesList.add(buffer);
                bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
            }

            Iterator<QueryExecuteDataSetWrapper> it = dataSetWrappers.iterator();
            Set<QueryExecuteDataSetWrapper> deletedDataSetWrappers = new HashSet<>();
            while (it.hasNext()) {
                QueryExecuteDataSetWrapper dataSetWrapper = it.next();
                if (dataSetWrapper.getTimestamp() == timestamp) {
                    if (dataSetWrapper.hasNext()) {
                        dataSetWrapper.next();
                    } else {
                        dataSetWrapper.close();
                        deletedDataSetWrappers.add(dataSetWrapper);
                        it.remove();
                    }
                }
            }
            for (QueryExecuteDataSetWrapper dataSetWrapper : deletedDataSetWrappers) {
                List<String> columnNames = dataSetWrapper.getColumnNames();
                for (String columnName : columnNames) {
                    columnSourcesList.get(columnName).remove(dataSetWrapper);
                }
            }
        }

        resp.setPaths(columnNameList);
        resp.setDataTypeList(columnTypeList);
        resp.setQueryDataSet(new QueryDataSet(ByteUtils.getColumnByteBuffer(timestamps.toArray(), DataType.LONG),
                valuesList, bitmapList));
    }
}
