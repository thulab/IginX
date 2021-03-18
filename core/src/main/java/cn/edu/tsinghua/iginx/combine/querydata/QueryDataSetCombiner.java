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
package cn.edu.tsinghua.iginx.combine.querydata;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryDataSetCombiner {

    private static final Logger logger = LoggerFactory.getLogger(QueryDataSetCombiner.class);

    private static final QueryDataSetCombiner instance = new QueryDataSetCombiner();

    private QueryDataSetCombiner() {
    }

    public static QueryDataSetCombiner getInstance() {
        return instance;
    }

    public void combineResult(QueryDataResp resp, List<QueryDataPlanExecuteResult> planExecuteResults) throws ExecutionException {
        Set<QueryExecuteDataSetWrapper> dataSetWrappers = planExecuteResults.stream().filter(e -> e.getQueryExecuteDataSet() != null)
                .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode())
                .map(QueryDataPlanExecuteResult::getQueryExecuteDataSet)
                .map(CheckedFunction.wrap(QueryExecuteDataSetWrapper::new))
                .collect(Collectors.toSet());

        List<String> columnNameList = new ArrayList<>();
        List<DataType> columnTypeList = new ArrayList<>();
        List<List<QueryExecuteDataSetWrapper>> columnSourcesList = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        List<ByteBuffer> valuesList = new ArrayList<>();
        List<ByteBuffer> bitmapList = new ArrayList<>();
        // 从序列名到 column 列表位置的映射，同时也负责检测某个列是否已经加入到列表中
        Map<String, Integer> columnPositionMap = new HashMap<>();
        // 初始化列集合
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
                    columnSourcesList.add(new ArrayList<>());
                }
                columnSourcesList.get(columnSourcesList.size() - 1).add(dataSetWrapper);
            }
        }
        // 初始化各个数据源
        // 在加载完一轮数据之后，把更新加载过数据的时间
        {
            Iterator<QueryExecuteDataSetWrapper> it = dataSetWrappers.iterator();
            Set<QueryExecuteDataSetWrapper> deletedDataSetWrappers = new HashSet<>();
            while(it.hasNext()) {
                QueryExecuteDataSetWrapper dataSetWrapper = it.next();
                if (dataSetWrapper.hasNext()) {
                    dataSetWrapper.next();
                } else { // 如果没有下一行，应该把当前数据集给移除掉
                    dataSetWrapper.close();
                    deletedDataSetWrappers.add(dataSetWrapper);
                    it.remove();
                }
            }
            // 删除掉已经空的 data source
            for (QueryExecuteDataSetWrapper dataSetWrapper : deletedDataSetWrappers) {
                List<String> columnNames = dataSetWrapper.getColumnNames();
                for (String columnName : columnNames) {
                    int index = columnPositionMap.get(columnName);
                    columnSourcesList.get(index).remove(dataSetWrapper);
                }
            }
        }

        while(!dataSetWrappers.isEmpty()) {
            long timestamp = Long.MAX_VALUE;
            // 顺序访问所有的还有数据数据的 timestamp，获取当前的时间戳
            for (QueryExecuteDataSetWrapper dataSetWrapper : dataSetWrappers) {
                timestamp = Math.min(dataSetWrapper.getTimestamp(), timestamp);
            }
            timestamps.add(timestamp);
            // 当前的行对应的数据
            Object[] values = new Object[columnTypeList.size()];
            Bitmap bitmap = new Bitmap(columnTypeList.size());
            for (int i = 0; i < columnTypeList.size(); i++) {
                String columnName = columnNameList.get(i);
                List<QueryExecuteDataSetWrapper> columnSources = columnSourcesList.get(i);
                for (QueryExecuteDataSetWrapper dataSetWrapper : columnSources) {
                    if (dataSetWrapper.getTimestamp() == timestamp) {
                        // 在检查某个字段时候，发现了时间戳符合的数据
                        Object value = dataSetWrapper.getValue(columnName);
                        if (value != null) {
                            // 时间戳符合更新 bitmap
                            values[i] = value;
                            bitmap.mark(i);
                            break;
                        }
                    }
                }
            }
            ByteBuffer buffer = ByteUtils.getByteBuffer(values, columnTypeList);
            valuesList.add(buffer);
            bitmapList.add(ByteBuffer.wrap(bitmap.getBytes()));
            // 在加载完一轮数据之后，把更新加载过数据的时间
            Iterator<QueryExecuteDataSetWrapper> it = dataSetWrappers.iterator();
            Set<QueryExecuteDataSetWrapper> deletedDataSetWrappers = new HashSet<>();
            while(it.hasNext()) {
                QueryExecuteDataSetWrapper dataSetWrapper = it.next();
                if (dataSetWrapper.getTimestamp() == timestamp) { // 如果时间戳是当前的时间戳，则意味着本行读完了，加载下一行
                    if (dataSetWrapper.hasNext()) {
                        dataSetWrapper.next();
                    } else { // 如果没有下一行，应该把当前数据集给移除掉
                        dataSetWrapper.close();
                        deletedDataSetWrappers.add(dataSetWrapper);
                        it.remove();
                    }
                }
            }
            // 删除掉已经空的 data source
            for (QueryExecuteDataSetWrapper dataSetWrapper : deletedDataSetWrappers) {
                List<String> columnNames = dataSetWrapper.getColumnNames();
                for (String columnName : columnNames) {
                    int index = columnPositionMap.get(columnName);
                    columnSourcesList.get(index).remove(dataSetWrapper);
                }
            }
        }
        resp.setPaths(columnNameList);
        resp.setDataTypeList(columnTypeList);
        resp.setQueryDataSet(new QueryDataSet(ByteUtils.getByteBuffer(timestamps.toArray(), DataType.LONG),
                valuesList, bitmapList));
    }

}
