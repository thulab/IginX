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
package cn.edu.tsinghua.iginx.conf;

public class Config {

    /**
     * iginx 配置
     */
    private String ip = "0.0.0.0";

    private int port = 6888;

    private String username = "root";

    private String password = "root";

    private int maxAsyncRetryTimes = 3;

    private int asyncExecuteThreadPoolSize = 20;

    private int syncExecuteThreadPoolSize = 60;

    private int replicaNum = 1;

    private boolean enableStatisticsCollection = false;

    private String statisticsCollectorClassName = "cn.edu.tsinghua.iginx.statistics.StatisticsCollector";

    private int statisticsLogInterval = 1000;

    /**
     * 元数据配置
     */
    private String metaStorage = "zookeeper";

    private String zookeeperConnectionString = "127.0.0.1:2181";

    private String fileDataDir = "meta";

    private String etcdEndpoints = "http://localhost:2379";

    /**
     * 策略配置
     */
    private String policyClassName = "cn.edu.tsinghua.iginx.policy.naive.NativePolicy";

    private long disorderMargin = 10;

    /**
     * 底层数据库配置
     */
    private String storageEngineList = "127.0.0.1#6667#iotdb#username=root#password=root#session_pool_size=100";

    private String databaseClassNames = "iotdb=cn.edu.tsinghua.iginx.iotdb.IoTDBPlanExecutor,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBPlanExecutor";

    /**
     * InfluxDB 配置
     */
    private String influxDBToken = "your-token";

    private String influxDBOrganizationName = "my-org";

    /**
     * Rest 服务配置
     */
    private boolean enableRestService = false;

    private String restIp = "0.0.0.0";

    private int restPort = 6666;

    private int timeseriesMaxTagSize = 10;

    /**
     * MQTT 服务配置
     */
    private boolean enableMQTT = false;

    private String mqttHost = "0.0.0.0";

    private int mqttPort = 1883;

    private int mqttHandlerPoolSize = 1;

    private String mqttPayloadFormatter = "cn.edu.tsinghua.iginx.mqtt.JsonPayloadFormatter";

    private int mqttMaxMessageSize = 1048576;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxAsyncRetryTimes() {
        return maxAsyncRetryTimes;
    }

    public void setMaxAsyncRetryTimes(int maxAsyncRetryTimes) {
        this.maxAsyncRetryTimes = maxAsyncRetryTimes;
    }

    public int getAsyncExecuteThreadPoolSize() {
        return asyncExecuteThreadPoolSize;
    }

    public void setAsyncExecuteThreadPoolSize(int asyncExecuteThreadPoolSize) {
        this.asyncExecuteThreadPoolSize = asyncExecuteThreadPoolSize;
    }

    public int getSyncExecuteThreadPoolSize() {
        return syncExecuteThreadPoolSize;
    }

    public void setSyncExecuteThreadPoolSize(int syncExecuteThreadPoolSize) {
        this.syncExecuteThreadPoolSize = syncExecuteThreadPoolSize;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }

    public boolean isEnableStatisticsCollection() {
        return enableStatisticsCollection;
    }

    public void setEnableStatisticsCollection(boolean enableStatisticsCollection) {
        this.enableStatisticsCollection = enableStatisticsCollection;
    }

    public String getStatisticsCollectorClassName() {
        return statisticsCollectorClassName;
    }

    public void setStatisticsCollectorClassName(String statisticsCollectorClassName) {
        this.statisticsCollectorClassName = statisticsCollectorClassName;
    }

    public int getStatisticsLogInterval() {
        return statisticsLogInterval;
    }

    public void setStatisticsLogInterval(int statisticsLogInterval) {
        this.statisticsLogInterval = statisticsLogInterval;
    }

    public String getMetaStorage() {
        return metaStorage;
    }

    public void setMetaStorage(String metaStorage) {
        this.metaStorage = metaStorage;
    }

    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }

    public void setZookeeperConnectionString(String zookeeperConnectionString) {
        this.zookeeperConnectionString = zookeeperConnectionString;
    }

    public String getFileDataDir() {
        return fileDataDir;
    }

    public void setFileDataDir(String fileDataDir) {
        this.fileDataDir = fileDataDir;
    }

    public String getEtcdEndpoints() {
        return etcdEndpoints;
    }

    public void setEtcdEndpoints(String etcdEndpoints) {
        this.etcdEndpoints = etcdEndpoints;
    }

    public String getPolicyClassName() {
        return policyClassName;
    }

    public void setPolicyClassName(String policyClassName) {
        this.policyClassName = policyClassName;
    }

    public long getDisorderMargin() {
        return disorderMargin;
    }

    public void setDisorderMargin(long disorderMargin) {
        this.disorderMargin = disorderMargin;
    }

    public String getStorageEngineList() {
        return storageEngineList;
    }

    public void setStorageEngineList(String storageEngineList) {
        this.storageEngineList = storageEngineList;
    }

    public String getDatabaseClassNames() {
        return databaseClassNames;
    }

    public void setDatabaseClassNames(String databaseClassNames) {
        this.databaseClassNames = databaseClassNames;
    }

    public String getInfluxDBToken() {
        return influxDBToken;
    }

    public void setInfluxDBToken(String influxDBToken) {
        this.influxDBToken = influxDBToken;
    }

    public String getInfluxDBOrganizationName() {
        return influxDBOrganizationName;
    }

    public void setInfluxDBOrganizationName(String influxDBOrganizationName) {
        this.influxDBOrganizationName = influxDBOrganizationName;
    }

    public boolean isEnableRestService() {
        return enableRestService;
    }

    public void setEnableRestService(boolean enableRestService) {
        this.enableRestService = enableRestService;
    }

    public String getRestIp() {
        return restIp;
    }

    public void setRestIp(String restIp) {
        this.restIp = restIp;
    }

    public int getRestPort() {
        return restPort;
    }

    public void setRestPort(int restPort) {
        this.restPort = restPort;
    }

    public int getTimeseriesMaxTagSize() {
        return timeseriesMaxTagSize;
    }

    public void setTimeseriesMaxTagSize(int timeseriesMaxTagSize) {
        this.timeseriesMaxTagSize = timeseriesMaxTagSize;
    }

    public boolean isEnableMQTT() {
        return enableMQTT;
    }

    public void setEnableMQTT(boolean enableMQTT) {
        this.enableMQTT = enableMQTT;
    }

    public String getMqttHost() {
        return mqttHost;
    }

    public void setMqttHost(String mqttHost) {
        this.mqttHost = mqttHost;
    }

    public int getMqttPort() {
        return mqttPort;
    }

    public void setMqttPort(int mqttPort) {
        this.mqttPort = mqttPort;
    }

    public int getMqttHandlerPoolSize() {
        return mqttHandlerPoolSize;
    }

    public void setMqttHandlerPoolSize(int mqttHandlerPoolSize) {
        this.mqttHandlerPoolSize = mqttHandlerPoolSize;
    }

    public String getMqttPayloadFormatter() {
        return mqttPayloadFormatter;
    }

    public void setMqttPayloadFormatter(String mqttPayloadFormatter) {
        this.mqttPayloadFormatter = mqttPayloadFormatter;
    }

    public int getMqttMaxMessageSize() {
        return mqttMaxMessageSize;
    }

    public void setMqttMaxMessageSize(int mqttMaxMessageSize) {
        this.mqttMaxMessageSize = mqttMaxMessageSize;
    }
}
