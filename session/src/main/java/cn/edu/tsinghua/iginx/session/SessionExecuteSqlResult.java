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

import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

public class SessionExecuteSqlResult {

    private SqlType sqlType;
    private AggregateType aggregateType;
    private long[] timestamps;
    private List<String> paths;
    private List<List<Object>> values;
    private List<DataType> dataTypeList;
    private int replicaNum;
    private long pointsNum;
    private String parseErrorMsg;
    private List<IginxInfo> iginxInfos;
    private List<StorageEngineInfo> storageEngineInfos;
    private List<MetaStorageInfo> metaStorageInfos;
    private LocalMetaStorageInfo localMetaStorageInfo;
    private List<RegisterTaskInfo> registerTaskInfos;
    private long jobId;
    private JobState jobState;

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
            case Query:
                constructQueryResult(resp);
                break;
            case ShowTimeSeries:
                this.paths = resp.getPaths();
                this.dataTypeList = resp.getDataTypeList();
                break;
            case ShowClusterInfo:
                this.iginxInfos = resp.getIginxInfos();
                this.storageEngineInfos = resp.getStorageEngineInfos();
                this.metaStorageInfos = resp.getMetaStorageInfos();
                this.localMetaStorageInfo = resp.getLocalMetaStorageInfo();
                break;
            case ShowRegisterTask:
                this.registerTaskInfos = resp.getRegisterTaskInfos();
                break;
            case CommitTransformJob:
                this.jobId = resp.getJobId();
                break;
            case ShowJobStatus:
                this.jobState = resp.getJobState();
                break;
            default:
                break;
        }
    }

    private void constructQueryResult(ExecuteSqlResp resp) {
        this.paths = resp.getPaths();
        this.dataTypeList = resp.getDataTypeList();

        if (resp.timestamps != null) {
            this.timestamps = getLongArrayFromByteBuffer(resp.timestamps);
        }

        // parse values
        if (resp.getQueryDataSet() != null) {
            this.values = parseValues(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
        } else {
            this.values = new ArrayList<>();
        }

        sortColumns();
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

    private void sortColumns() {
        Map<String, DataType> typeMap = new TreeMap<>();
        Map<String, List<Object>> valueMap = new TreeMap<>();
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            typeMap.put(path, dataTypeList.get(i));
            for (int j = 0; j < values.size(); j++) {
                List<Object> colValue = valueMap.get(path);
                if (colValue == null) {
                    List<Object> list = new ArrayList<>(Collections.singletonList(values.get(j).get(i)));
                    valueMap.put(path, list);
                } else {
                    colValue.add(values.get(j).get(i));
                }
            }
        }
        this.paths = new ArrayList<>(typeMap.keySet());
        this.dataTypeList = new ArrayList<>(typeMap.values());
        List<List<Object>> newValues = new ArrayList<>();
        for (int i = 0; i < values.size(); i++)
            newValues.add(new ArrayList<>());
        for (String key : valueMap.keySet()) {
            for (int i = 0; i < newValues.size(); i++) {
                newValues.get(i).add(valueMap.get(key).get(i));
            }
        }
        this.values = newValues;
    }

    public List<List<String>> getResultInList(boolean needFormatTime, String timePrecision) {
        List<List<String>> result = new ArrayList<>();
        if (isQuery()) {
            List<Integer> maxSizeList = new ArrayList<>();
            result = cacheResult(needFormatTime, timePrecision, maxSizeList);
        } else if (sqlType == SqlType.ShowTimeSeries) {
            result.add(new ArrayList<>(Arrays.asList("Path", "DataType")));
            if (paths != null) {
                for (int i = 0; i < paths.size(); i++) {
                    result.add(Arrays.asList(paths.get(i) + "", dataTypeList.get(i) + ""));
                }
            }
        } else if (sqlType == SqlType.GetReplicaNum) {
            result.add(new ArrayList<>(Collections.singletonList("Replica num")));
            result.add(new ArrayList<>(Collections.singletonList(replicaNum + "")));
        } else if (sqlType == SqlType.CountPoints) {
            result.add(new ArrayList<>(Collections.singletonList("Points num")));
            result.add(new ArrayList<>(Collections.singletonList(pointsNum + "")));
        } else {
            result.add(new ArrayList<>(Collections.singletonList("Empty set")));
        }
        return result;
    }

    public void print(boolean needFormatTime, String timePrecision) {
        System.out.print(getResultInString(needFormatTime, timePrecision));
    }

    public String getResultInString(boolean needFormatTime, String timePrecision) {
        if (isQuery()) {
            if (aggregateType == AggregateType.LAST) {
                return buildLastQueryResult(needFormatTime, timePrecision);
            }
            return buildQueryResult(needFormatTime, timePrecision);
        } else if (sqlType == SqlType.ShowTimeSeries) {
            return buildShowTimeSeriesResult();
        } else if (sqlType == SqlType.ShowClusterInfo) {
            return buildShowClusterInfoResult();
        } else if (sqlType == SqlType.ShowRegisterTask) {
            return buildShowRegisterTaskResult();
        } else if (sqlType == SqlType.GetReplicaNum) {
            return "Replica num: " + replicaNum + "\n";
        } else if (sqlType == SqlType.CountPoints) {
            return "Points num: " + pointsNum + "\n";
        } else {
            return "No data to print." + "\n";
        }
    }

    private String buildQueryResult(boolean needFormatTime, String timePrecision) {
        StringBuilder builder = new StringBuilder();
        builder.append("ResultSets:").append("\n");

        List<Integer> maxSizeList = new ArrayList<>();
        List<List<String>> cache = cacheResult(needFormatTime, timePrecision, maxSizeList);

        builder.append(buildBlockLine(maxSizeList));
        builder.append(buildRow(cache, 0, maxSizeList));
        builder.append(buildBlockLine(maxSizeList));
        for (int i = 1; i < cache.size(); i++) {
            builder.append(buildRow(cache, i, maxSizeList));
        }
        builder.append(buildBlockLine(maxSizeList));

        builder.append(buildCount(cache.size() - 1));

        return builder.toString();
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
            label.add(path);
            maxSizeList.add(path.length());
        }

        for (int i = 0; i < values.size(); i++) {
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
                String rowValue = valueToString(rowData.get(j));
                rowCache.add(rowValue);

                int index = timestamps == null ? j : j + 1;
                if (maxSizeList.get(index) < rowValue.length()) {
                    maxSizeList.set(index, rowValue.length());
                }
            }
            cache.add(rowCache);
        }

        cache.add(0, label);

        return cache;
    }

    private String buildBlockLine(List<Integer> maxSizeList) {
        StringBuilder blockLine = new StringBuilder();
        for (Integer integer : maxSizeList) {
            blockLine.append("+").append(StringUtils.repeat("-", integer));
        }
        blockLine.append("+").append("\n");
        return blockLine.toString();
    }

    private String buildRow(List<List<String>> cache, int rowIdx, List<Integer> maxSizeList) {
        StringBuilder builder = new StringBuilder();
        builder.append("|");
        int maxSize;
        String rowValue;
        for (int i = 0; i < maxSizeList.size(); i++) {
            maxSize = maxSizeList.get(i);
            rowValue = cache.get(rowIdx).get(i);
            builder.append(String.format("%" + maxSize + "s|", rowValue));
        }
        builder.append("\n");
        return builder.toString();
    }

    public String buildCount(int count) {
        if (count <= 0) {
            return "Empty set.\n";
        } else {
            return "Total line number = " + count + "\n";
        }
    }

    private String buildLastQueryResult(boolean needFormatTime, String timePrecision) {
        StringBuilder builder = new StringBuilder();
        builder.append("ResultSets:").append("\n");
        int num = paths == null ? 0 : paths.size();
        if (values != null && !values.isEmpty()) {
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("Time", "Path", "value")));
            for (int i = 0; i < paths.size(); i++) {
                cache.add(new ArrayList<>(Arrays.asList(
                    needFormatTime ? formatTime(timestamps[i], timePrecision) : String.valueOf(timestamps[i]),
                    paths.get(i),
                    valueToString(values.get(0).get(i))
                )));
            }

            buildFromStringList(builder, cache);
        }
        builder.append(buildCount(num));
        return builder.toString();
    }

    private String buildShowTimeSeriesResult() {
        StringBuilder builder = new StringBuilder();
        builder.append("Time series:").append("\n");
        int num = paths == null ? 0 : paths.size();
        if (paths != null) {
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("Path", "DataType")));
            for (int i = 0; i < paths.size(); i++) {
                cache.add(new ArrayList<>(Arrays.asList(paths.get(i), dataTypeList.get(i).toString())));
            }

            buildFromStringList(builder, cache);
        }
        builder.append(buildCount(num));
        return builder.toString();
    }

    private String buildShowClusterInfoResult() {
        StringBuilder builder = new StringBuilder();

        if (iginxInfos != null && !iginxInfos.isEmpty()) {
            builder.append("IginX infos:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("ID", "IP", "PORT")));
            for (int i = 0; i < iginxInfos.size(); i++) {
                IginxInfo info = iginxInfos.get(i);
                cache.add(new ArrayList<>(Arrays.asList(
                    String.valueOf(info.getId()),
                    info.getIp(),
                    String.valueOf(info.getPort())
                )));
            }
            buildFromStringList(builder, cache);
        }

        if (storageEngineInfos != null && !storageEngineInfos.isEmpty()) {
            builder.append("Storage engine infos:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("ID", "IP", "PORT", "TYPE")));
            for (int i = 0; i < storageEngineInfos.size(); i++) {
                StorageEngineInfo info = storageEngineInfos.get(i);
                cache.add(new ArrayList<>(Arrays.asList(
                    String.valueOf(info.getId()),
                    info.getIp(),
                    String.valueOf(info.getPort()),
                    info.getType()
                )));
            }
            buildFromStringList(builder, cache);
        }

        if (metaStorageInfos != null && !metaStorageInfos.isEmpty()) {
            builder.append("Meta Storage infos:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("IP", "PORT", "TYPE")));
            for (int i = 0; i < metaStorageInfos.size(); i++) {
                MetaStorageInfo info = metaStorageInfos.get(i);
                cache.add(new ArrayList<>(Arrays.asList(
                    info.getIp(),
                    String.valueOf(info.getPort()),
                    info.getType()
                )));
            }
            buildFromStringList(builder, cache);
        }

        if (localMetaStorageInfo != null) {
            builder.append("Meta Storage path:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Collections.singletonList("PATH")));
            cache.add(new ArrayList<>(Collections.singletonList(localMetaStorageInfo.getPath())));
            buildFromStringList(builder, cache);
        }

        return builder.toString();
    }

    private String buildShowRegisterTaskResult() {
        StringBuilder builder = new StringBuilder();

        if (registerTaskInfos != null && !registerTaskInfos.isEmpty()) {
            builder.append("Register task infos:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("NAME", "CLASS_NAME", "FILE_NAME", "IP", "UDF_TYPE")));
            for (int i = 0; i < registerTaskInfos.size(); i++) {
                RegisterTaskInfo info = registerTaskInfos.get(i);
                cache.add(new ArrayList<>(Arrays.asList(
                    info.getName(),
                    info.getClassName(),
                    info.getFileName(),
                    info.getIp(),
                    info.getType().toString()
                )));
            }
            buildFromStringList(builder, cache);
        }

        return builder.toString();
    }

    private void buildFromStringList(StringBuilder builder, List<List<String>> cache) {
        List<Integer> maxSizeList = new ArrayList<>();
        if (!cache.isEmpty()) {
            int colCount = cache.get(0).size();
            for (int i = 0; i < colCount; i++) {
                maxSizeList.add(0);
            }
            for (List<String> row : cache) {
                for (int i = 0; i < colCount; i++) {
                    maxSizeList.set(i, Math.max(row.get(i).length(), maxSizeList.get(i)));
                }
            }
            builder.append(buildBlockLine(maxSizeList));
            builder.append(buildRow(cache, 0, maxSizeList));
            builder.append(buildBlockLine(maxSizeList));
            for (int i = 1; i < cache.size(); i++) {
                builder.append(buildRow(cache, i, maxSizeList));
            }
            builder.append(buildBlockLine(maxSizeList));
        }
    }

    private String valueToString(Object value) {
        String ret;
        if (value instanceof byte[]) {
            ret = new String((byte[]) value);
        } else {
            ret = String.valueOf(value);
        }
        return ret;
    }

    private String formatTime(long timestamp, String timePrecision) {
        long timeInMs;
        switch (timePrecision) {
            case "s":
                timeInMs = timestamp * 1000;
                break;
            case "us":
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
        return sqlType == SqlType.Query;
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

    public long getPointsNum() {
        return pointsNum;
    }

    public String getParseErrorMsg() {
        return parseErrorMsg;
    }

    public long getJobId() {
        return jobId;
    }

    public JobState getJobState() {
        return jobState;
    }
}
