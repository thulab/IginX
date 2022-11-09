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
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.*;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.*;

public class SessionExecuteSqlResult {

    public static final String DEFAULT_TIME_FORMAT = "default_time_format";

    private SqlType sqlType;
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
    private List<Long> jobIdList;

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
            case CountPoints: // TODO 需要在底层屏蔽系统级时间序列以及注释索引数据点
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
            case ShowEligibleJob:
                this.jobIdList = resp.getJobIdList();
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
            this.values = getValuesFromBufferAndBitmaps(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
        } else {
            this.values = new ArrayList<>();
        }
    }

    public List<List<String>> getResultInList(boolean needFormatTime, String timeFormat,
                                              String timePrecision) {
        List<List<String>> result = new ArrayList<>();
        if (isQuery()) {
            List<Integer> maxSizeList = new ArrayList<>();
            result = cacheResult(needFormatTime, timeFormat, timePrecision, maxSizeList);
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
            return buildQueryResult(needFormatTime, timePrecision);
        } else if (sqlType == SqlType.ShowTimeSeries) {
            return buildShowTimeSeriesResult();
        } else if (sqlType == SqlType.ShowClusterInfo) {
            return buildShowClusterInfoResult();
        } else if (sqlType == SqlType.ShowRegisterTask) {
            return buildShowRegisterTaskResult();
        } else if (sqlType == SqlType.ShowEligibleJob) {
            return buildShowEligibleJobResult();
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
        List<List<String>> cache = cacheResult(needFormatTime, DEFAULT_TIME_FORMAT, timePrecision, maxSizeList);

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

    private List<List<String>> cacheResult(boolean needFormatTime, String timeFormat,
                                           String timePrecision, List<Integer> maxSizeList) {
        List<List<String>> cache = new ArrayList<>();
        List<String> label = new ArrayList<>();
        int annotationPathIndex = -1;
        if (timestamps != null) {
            label.add("Time");
            maxSizeList.add(4);
        }
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            if (!path.equals("TITLE.DESCRIPTION")) { // TODO 不展示系统级时间序列
                label.add(path);
                maxSizeList.add(path.length());
            } else {
                annotationPathIndex = i;
            }
        }

        for (int i = 0; i < values.size(); i++) {
            List<String> rowCache = new ArrayList<>();
            if (timestamps != null) {
                if (timestamps[i] == Long.MAX_VALUE - 1 || timestamps[i] == Long.MAX_VALUE - 2) {
                    continue;
                }
                String timeValue;
                if (needFormatTime) {
                    timeValue = formatTime(timestamps[i], timeFormat, timePrecision);
                } else {
                    timeValue = String.valueOf(timestamps[i]);
                }
                rowCache.add(timeValue);
                if (maxSizeList.get(0) < timeValue.length()) {
                    maxSizeList.set(0, timeValue.length());
                }
            }

            List<Object> rowData = values.get(i);
            int num = 0;
            boolean isNull = true; // TODO 该行除系统级时间序列之外全部为空
            for (int j = 0; j < rowData.size(); j++) {
                if (j == annotationPathIndex) {
                    continue;
                }
                String rowValue = valueToString(rowData.get(j));
                rowCache.add(rowValue);
                if (!rowValue.equalsIgnoreCase("null")) {
                    isNull = false;
                }

                int index = timestamps == null ? num : num + 1;
                if (maxSizeList.get(index) < rowValue.length()) {
                    maxSizeList.set(index, rowValue.length());
                }
                num++;
            }
            if (!isNull) {
                cache.add(rowCache);
            }
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

    private String buildShowTimeSeriesResult() {
        StringBuilder builder = new StringBuilder();
        builder.append("Time series:").append("\n");
        int num = 0;
        if (paths != null) {
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("Path", "DataType")));
            for (int i = 0; i < paths.size(); i++) {
                if (!paths.get(i).equals("TITLE.DESCRIPTION")) { // TODO 不展示系统级时间序列
                    cache.add(new ArrayList<>(Arrays.asList(paths.get(i), dataTypeList.get(i).toString())));
                    num++;
                }
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

    private String buildShowEligibleJobResult() {
        StringBuilder builder = new StringBuilder();

        if (jobIdList != null) {
            builder.append("Transform Id List:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Collections.singletonList("JobIdList")));
            for (long jobId : jobIdList) {
                cache.add(new ArrayList<>(Collections.singletonList(String.valueOf(jobId))));
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

    private String formatTime(long timestamp, String timeFormat, String timePrecision) {
        long timeInMs = TimeUtils.getTimeInMs(timestamp, timePrecision);
        if (timeFormat.equals(DEFAULT_TIME_FORMAT)) {
            return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(timeInMs);
        } else {
            return new SimpleDateFormat(timeFormat).format(timeInMs);
        }
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
