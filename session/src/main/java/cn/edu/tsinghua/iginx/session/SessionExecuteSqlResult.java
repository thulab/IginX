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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

public class SessionExecuteSqlResult {

    private SqlType sqlType;
    private AggregateType aggregateType;
    private long[] timestamps;
    private List<String> paths;
    private String orderByPath;
    private List<List<Object>> values;
    private List<DataType> dataTypeList;
    private int replicaNum;
    private long pointsNum;
    private String parseErrorMsg;
    private int limit;
    private int offset;
    private boolean ascending;

    // Only for mock test
    public SessionExecuteSqlResult() {
    }

    public SessionExecuteSqlResult(ExecuteSqlResp resp) {
        this.sqlType = resp.getType();
        this.parseErrorMsg = resp.getParseErrorMsg();
        switch (resp.getType()) {
            case GetReplicaNum:
                this.replicaNum = resp.getReplicaNum();
                break;
            case CountPoints:
                this.pointsNum = resp.getPointsNum();
                break;
            case AggregateQuery:
            case SimpleQuery:
            case DownsampleQuery:
            case ValueFilterQuery:
                constructQueryResult(resp);
                break;
            case ShowTimeSeries:
                this.paths = resp.getPaths();
                this.dataTypeList = resp.getDataTypeList();
                break;
            default:
                break;
        }
    }

    private void constructQueryResult(ExecuteSqlResp resp) {
        this.paths = resp.getPaths();
        this.dataTypeList = resp.getDataTypeList();
        this.limit = resp.getLimit();
        this.offset = resp.getOffset();
        this.orderByPath = resp.getOrderByPath();
        this.ascending = resp.isAscending();

        if (resp.timestamps != null) {
            this.timestamps = getLongArrayFromByteBuffer(resp.timestamps);
        }
        if (resp.queryDataSet != null && resp.queryDataSet.timestamps != null) {
            this.timestamps = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
        }

        if (resp.getType() == SqlType.AggregateQuery ||
                resp.getType() == SqlType.DownsampleQuery) {
            this.aggregateType = resp.aggregateType;
        }

        // parse values
        if (resp.getType() == SqlType.AggregateQuery) {
            Object[] aggregateValues = ByteUtils.getValuesByDataType(resp.valuesList, resp.dataTypeList);
            List<Object> aggregateValueList = new ArrayList<>(Arrays.asList(aggregateValues));
            this.values = new ArrayList<>();
            this.values.add(aggregateValueList);
        } else {
            this.values = parseValues(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
        }
    }

    private List<List<Object>> parseValues(List<DataType> dataTypeList, List<ByteBuffer> valuesList, List<ByteBuffer> bitmapList) {
        List<List<Object>> res = new ArrayList<>();
        for (int i = 0; i < valuesList.size(); i++) {
            List<Object> tempValues = new ArrayList<>();
            ByteBuffer valuesBuffer = valuesList.get(i);
            ByteBuffer bitmapBuffer = bitmapList.get(i);
            Bitmap bitmap = new Bitmap(dataTypeList.size(), bitmapBuffer.array());
            for (int j = 0; j < dataTypeList.size(); j++) {
                if (bitmap.get(j)) {
                    tempValues.add(getValueFromByteBufferByDataType(valuesBuffer, dataTypeList.get(j)));
                } else {
                    tempValues.add(null);
                }
            }
            res.add(tempValues);
        }
        return res;
    }

    public void print(boolean needFormatTime, String timePrecision) {
        if (isQuery()) {
            printQueryResult(needFormatTime, timePrecision);
        } else if (sqlType == SqlType.ShowTimeSeries) {
            printShowTimeSeriesResult();
        } else if (sqlType == SqlType.GetReplicaNum) {
            System.out.println("Replica num:" + replicaNum);
        } else if (sqlType == SqlType.CountPoints) {
            System.out.println("Points num:" + pointsNum);
        } else {
            System.out.println("No data to print.");
        }
    }

    private void printQueryResult(boolean needFormatTime, String timePrecision) {
        System.out.printf("%s ResultSets:%n", sqlType.toString());

        List<Integer> maxSizeList = new ArrayList<>();
        List<List<String>> cache = cacheResult(needFormatTime, timePrecision, maxSizeList);

        printBlockLine(maxSizeList);
        printRow(cache, 0, maxSizeList);
        printBlockLine(maxSizeList);
        for (int i = 1; i < cache.size(); i++) {
            printRow(cache, i, maxSizeList);
        }
        printBlockLine(maxSizeList);

        printCount(cache.size() - 1);
    }

    private List<List<String>> cacheResult(boolean needFormatTime, String timePrecision,
                                           List<Integer> maxSizeList) {
        List<List<String>> cache = new ArrayList<>();
        List<String> label = new ArrayList<>();
        if (timestamps != null) {
            label.add("Time");
            maxSizeList.add(4);
        }
        for (String path : paths) {
            if (aggregateType == null) {
                label.add(path);
                maxSizeList.add(path.length());
            } else {
                String newLabel = aggregateType.toString() + "(" + path + ")";
                label.add(newLabel);
                maxSizeList.add(newLabel.length());
            }
        }


        int maxOutputLen = Math.min(offset + limit, values.size());
        for (int i = offset; i < maxOutputLen; i++) {
            List<String> rowCache = new ArrayList<>();
            if (timestamps != null) {
                String timeValue;
                if (needFormatTime) {
                    timeValue = formatTime(timestamps[i], timePrecision);
                } else {
                    timeValue = String.valueOf(timestamps[i]);
                }
                rowCache.add(timeValue);
                if (maxSizeList.get(0) < timeValue.length()) {
                    maxSizeList.set(0, timeValue.length());
                }
            }

            List<Object> rowData = values.get(i);
            for (int j = 0; j < rowData.size(); j++) {
                Object rowDatum = rowData.get(j);
                String rowValue;
                if (rowDatum instanceof byte[]) {
                    rowValue = new String((byte[]) rowDatum);
                } else {
                    rowValue = String.valueOf(rowDatum);
                }
                rowCache.add(rowValue);

                int index = timestamps == null ? j : j + 1;
                if (maxSizeList.get(index) < rowValue.length()) {
                    maxSizeList.set(index, rowValue.length());
                }
            }
            cache.add(rowCache);
        }

        int index = paths.indexOf(orderByPath);
        if (orderByPath != null && !orderByPath.equals("") && index >= 0) {
            DataType type = dataTypeList.get(index);
            int finalIndex = timestamps == null ? index : ++index;
            cache = cache.stream()
                    .sorted((o1, o2) -> {
                        if (ascending) {
                            return compare(o1.get(finalIndex), o2.get(finalIndex), type);
                        } else {
                            return compare(o2.get(finalIndex), o1.get(finalIndex), type);
                        }
                    })
                    .collect(Collectors.toList());
        }

        cache.add(0, label);

        return cache;
    }

    private int compare(String s1, String s2, DataType type) {
        switch (type) {
            case LONG:
                return Long.compare(Long.parseLong(s1), Long.parseLong(s2));
            case FLOAT:
                return Float.compare(Float.parseFloat(s1), Float.parseFloat(s2));
            case DOUBLE:
                return Double.compare(Double.parseDouble(s1), Double.parseDouble(s2));
            case INTEGER:
                return Integer.compare(Integer.parseInt(s1), Integer.parseInt(s2));
            case BOOLEAN:
                return Boolean.compare(Boolean.parseBoolean(s1), Boolean.parseBoolean(s2));
            default:
                return s1.compareTo(s2);
        }
    }

    private void printBlockLine(List<Integer> maxSizeList) {
        StringBuilder blockLine = new StringBuilder();
        for (Integer integer : maxSizeList) {
            blockLine.append("+").append(StringUtils.repeat("-", integer));
        }
        blockLine.append("+");
        System.out.println(blockLine.toString());
    }

    private void printRow(List<List<String>> cache, int rowIdx, List<Integer> maxSizeList) {
        System.out.print("|");
        int maxSize;
        String rowValue;
        for (int i = 0; i < maxSizeList.size(); i++) {
            maxSize = maxSizeList.get(i);
            rowValue = cache.get(rowIdx).get(i);
            System.out.printf("%" + maxSize + "s|", rowValue);
        }
        System.out.println();
    }

    public void printCount(int count) {
        if (count <= 0) {
            System.out.println("Empty set.");
        } else {
            System.out.println("Total line number = " + count);
        }
    }

    private void printShowTimeSeriesResult() {
        int num = paths == null ? 0 : paths.size();
        if (paths != null) {
            for (int i = 0; i < paths.size(); i++) {
                System.out.println(String.format("TimeSeries{Path='%s', DataType='%s'}", paths.get(i), dataTypeList.get(i)));
            }
        }
        System.out.println("Total time series num = " + num);
    }

    private String formatTime(long timestamp, String timePrecision) {
        long timeInMs;
        switch (timePrecision) {
            case "s":
                timeInMs = timestamp * 1000;
                break;
            case "µs":
                timeInMs = timestamp / 1000;
                break;
            case "ns":
                timeInMs = timestamp / 1000000;
                break;
            default:
                timeInMs = timestamp;
        }
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(timeInMs);
    }

    public boolean isQuery() {
        return sqlType == SqlType.SimpleQuery ||
                sqlType == SqlType.AggregateQuery ||
                sqlType == SqlType.DownsampleQuery ||
                sqlType == SqlType.ValueFilterQuery;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    public AggregateType getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    public long[] getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(long[] timestamps) {
        this.timestamps = timestamps;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public List<List<Object>> getValues() {
        return values;
    }

    public void setValues(List<List<Object>> values) {
        this.values = values;
    }

    public List<DataType> getDataTypeList() {
        return dataTypeList;
    }

    public void setDataTypeList(List<DataType> dataTypeList) {
        this.dataTypeList = dataTypeList;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }

    public long getPointsNum() {
        return pointsNum;
    }

    public void setPointsNum(long pointsNum) {
        this.pointsNum = pointsNum;
    }

    public String getParseErrorMsg() {
        return parseErrorMsg;
    }

    public void setParseErrorMsg(String parseErrorMsg) {
        this.parseErrorMsg = parseErrorMsg;
    }

    public long getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
