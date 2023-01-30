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

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.FormatUtils;

import java.util.*;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.*;

public class SessionExecuteSqlResult {

    private SqlType sqlType;
    private long[] keys;
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
            this.keys = getLongArrayFromByteBuffer(resp.timestamps);
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
            result = cacheResult(needFormatTime, timeFormat, timePrecision);
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

        List<List<String>> cache = cacheResult(needFormatTime, FormatUtils.DEFAULT_TIME_FORMAT, timePrecision);
        builder.append(FormatUtils.formatResult(cache));

        builder.append(FormatUtils.formatCount(cache.size() - 1));
        return builder.toString();
    }

    private List<List<String>> cacheResult(boolean needFormatTime, String timeFormat,
                                           String timePrecision) {
        List<List<String>> cache = new ArrayList<>();
        List<String> label = new ArrayList<>();
        int annotationPathIndex = -1;
        if (keys != null) {
            label.add(GlobalConstant.KEY_NAME);
        }
        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            if (!path.equals("TITLE.DESCRIPTION")) { // TODO 不展示系统级时间序列
                label.add(path);
            } else {
                annotationPathIndex = i;
            }
        }

        for (int i = 0; i < values.size(); i++) {
            List<String> rowCache = new ArrayList<>();
            if (keys != null) {
                if (keys[i] == Long.MAX_VALUE - 1 || keys[i] == Long.MAX_VALUE - 2) {
                    continue;
                }
                String timeValue;
                if (needFormatTime) {
                    timeValue = FormatUtils.formatTime(keys[i], timeFormat, timePrecision);
                } else {
                    timeValue = String.valueOf(keys[i]);
                }
                rowCache.add(timeValue);
            }

            List<Object> rowData = values.get(i);
            boolean isNull = true; // TODO 该行除系统级时间序列之外全部为空
            for (int j = 0; j < rowData.size(); j++) {
                if (j == annotationPathIndex) {
                    continue;
                }
                String rowValue = FormatUtils.valueToString(rowData.get(j));
                rowCache.add(rowValue);
                if (!rowValue.equalsIgnoreCase("null")) {
                    isNull = false;
                }
            }
            if (!isNull) {
                cache.add(rowCache);
            }
        }

        cache.add(0, label);

        return cache;
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
            builder.append(FormatUtils.formatResult(cache));
        }
        builder.append(FormatUtils.formatCount(num));
        return builder.toString();
    }

    private String buildShowClusterInfoResult() {
        StringBuilder builder = new StringBuilder();

        if (iginxInfos != null && !iginxInfos.isEmpty()) {
            builder.append("IginX infos:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("ID", "IP", "PORT")));
            for (IginxInfo info : iginxInfos) {
                cache.add(new ArrayList<>(Arrays.asList(
                    String.valueOf(info.getId()),
                    info.getIp(),
                    String.valueOf(info.getPort())
                )));
            }
            builder.append(FormatUtils.formatResult(cache));
        }

        if (storageEngineInfos != null && !storageEngineInfos.isEmpty()) {
            builder.append("Storage engine infos:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("ID", "IP", "PORT", "TYPE", "SCHEMA_PREFIX", "DATAPREFIX")));
            for (StorageEngineInfo info : storageEngineInfos) {
                cache.add(new ArrayList<>(Arrays.asList(
                    String.valueOf(info.getId()),
                    info.getIp(),
                    String.valueOf(info.getPort()),
                    info.getType(),
                    info.getSchemaPrefix(),
                    info.getDataPrefix()
                )));
            }
            builder.append(FormatUtils.formatResult(cache));
        }

        if (metaStorageInfos != null && !metaStorageInfos.isEmpty()) {
            builder.append("Meta Storage infos:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("IP", "PORT", "TYPE")));
            for (MetaStorageInfo info : metaStorageInfos) {
                cache.add(new ArrayList<>(Arrays.asList(
                    info.getIp(),
                    String.valueOf(info.getPort()),
                    info.getType()
                )));
            }
            builder.append(FormatUtils.formatResult(cache));
        }

        if (localMetaStorageInfo != null) {
            builder.append("Meta Storage path:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Collections.singletonList("PATH")));
            cache.add(new ArrayList<>(Collections.singletonList(localMetaStorageInfo.getPath())));
            builder.append(FormatUtils.formatResult(cache));
        }

        return builder.toString();
    }

    private String buildShowRegisterTaskResult() {
        StringBuilder builder = new StringBuilder();

        if (registerTaskInfos != null && !registerTaskInfos.isEmpty()) {
            builder.append("Register task infos:").append("\n");
            List<List<String>> cache = new ArrayList<>();
            cache.add(new ArrayList<>(Arrays.asList("NAME", "CLASS_NAME", "FILE_NAME", "IP", "UDF_TYPE")));
            for (RegisterTaskInfo info : registerTaskInfos) {
                cache.add(new ArrayList<>(Arrays.asList(
                    info.getName(),
                    info.getClassName(),
                    info.getFileName(),
                    info.getIp(),
                    info.getType().toString()
                )));
            }
            builder.append(FormatUtils.formatResult(cache));
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
            builder.append(FormatUtils.formatResult(cache));
        }

        return builder.toString();
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

    public long[] getKeys() {
        return keys;
    }

    public void setKeys(long[] keys) {
        this.keys = keys;
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

    public List<RegisterTaskInfo> getRegisterTaskInfos() {
        return registerTaskInfos;
    }
}
