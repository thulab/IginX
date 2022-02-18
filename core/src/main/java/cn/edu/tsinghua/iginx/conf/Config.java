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

    private String ip = "0.0.0.0";

    private int port = 6888;

    private String username = "root";

    private String password = "root";

    private String metaStorage = "zookeeper";

    private String zookeeperConnectionString = "127.0.0.1:2181";

    private String storageEngineList = "127.0.0.1#6667#iotdb#username=root#password=root#sessionPoolSize=100";

    private int maxAsyncRetryTimes = 2;

    private int syncExecuteThreadPool = 60;

    private int asyncExecuteThreadPool = 20;

    private int replicaNum = 1;

    private String databaseClassNames = "iotdb=cn.edu.tsinghua.iginx.iotdb.IoTDBPlanExecutor,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBPlanExecutor";

    private String policyClassName = "cn.edu.tsinghua.iginx.policy.naive.NativePolicy";

    private int storageUnitNum = 30;

    private String statisticsCollectorClassName = "";

    private int statisticsLogInterval = 5000;

    private boolean enableEnvParameter = false;

    private String restIp = "127.0.0.1";

    private int restPort = 6666;

    private int maxTimeseriesLength = 10;

    private long disorderMargin = 10;

    private int asyncRestThreadPool = 100;

    private boolean enableRestService = true;

    private String fileDataDir = "";

    private String etcdEndpoints = "http://localhost:2379";

    private boolean enableMQTT = false;

    private String mqttHost = "0.0.0.0";

    private int mqttPort = 1883;

    private int mqttHandlerPoolSize = 1;

    private String mqttPayloadFormatter = "cn.edu.tsinghua.iginx.mqtt.JsonPayloadFormatter";

    private int mqttMaxMessageSize = 1048576;

    private String clients = "";

    private int instancesNumPerClient = 0;

    private String queryOptimizer = "";

    private String constraintChecker = "naive";

    private String physicalOptimizer = "naive";

    private int memoryTaskThreadPoolSize = 200;

    private int physicalTaskThreadPoolSizePerStorage = 100;

    private int maxCachedPhysicalTaskPerStorage = 500;

    private double cachedTimeseriesProb = 0.01;

    private int retryCount = 10;

    private int retryWait = 5000;

    private int reAllocatePeriod = 30000;

    private int fragmentPerEngine = 10;

    private boolean enableStorageGroupValueLimit = true;

    private double storageGroupValueLimit = 200.0;

    public int getMaxTimeseriesLength() {
        return maxTimeseriesLength;
    }

    public void setMaxTimeseriesLength(int maxTimeseriesLength) {
        this.maxTimeseriesLength = maxTimeseriesLength;
    }

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

    public String getZookeeperConnectionString() {
        return zookeeperConnectionString;
    }

    public void setZookeeperConnectionString(String zookeeperConnectionString) {
        this.zookeeperConnectionString = zookeeperConnectionString;
    }

    public String getStorageEngineList() {
        return storageEngineList;
    }

    public void setStorageEngineList(String storageEngineList) {
        this.storageEngineList = storageEngineList;
    }

    public int getMaxAsyncRetryTimes() {
        return maxAsyncRetryTimes;
    }

    public void setMaxAsyncRetryTimes(int maxAsyncRetryTimes) {
        this.maxAsyncRetryTimes = maxAsyncRetryTimes;
    }

    public int getSyncExecuteThreadPool() {
        return syncExecuteThreadPool;
    }

    public void setSyncExecuteThreadPool(int syncExecuteThreadPool) {
        this.syncExecuteThreadPool = syncExecuteThreadPool;
    }

    public int getAsyncExecuteThreadPool() {
        return asyncExecuteThreadPool;
    }

    public void setAsyncExecuteThreadPool(int asyncExecuteThreadPool) {
        this.asyncExecuteThreadPool = asyncExecuteThreadPool;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }

    public String getDatabaseClassNames() {
        return databaseClassNames;
    }

    public void setDatabaseClassNames(String databaseClassNames) {
        this.databaseClassNames = databaseClassNames;
    }

    public String getPolicyClassName() {
        return policyClassName;
    }

    public void setPolicyClassName(String policyClassName) {
        this.policyClassName = policyClassName;
    }

    public int getStorageUnitNum() {
        return storageUnitNum;
    }

    public void setStorageUnitNum(int storageUnitNum) {
        this.storageUnitNum = storageUnitNum;
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

    public boolean isEnableEnvParameter() {
        return enableEnvParameter;
    }

    public void setEnableEnvParameter(boolean enableEnvParameter) {
        this.enableEnvParameter = enableEnvParameter;
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

    public int getAsyncRestThreadPool() {
        return asyncRestThreadPool;
    }

    public void setAsyncRestThreadPool(int asyncRestThreadPool) {
        this.asyncRestThreadPool = asyncRestThreadPool;
    }

    public boolean isEnableRestService() {
        return enableRestService;
    }

    public void setEnableRestService(boolean enableRestService) {
        this.enableRestService = enableRestService;
    }

    public String getMetaStorage() {
        return metaStorage;
    }

    public void setMetaStorage(String metaStorage) {
        this.metaStorage = metaStorage;
    }

    public String getFileDataDir() {
        return fileDataDir;
    }

    public void setFileDataDir(String fileDataDir) {
        this.fileDataDir = fileDataDir;
    }

    public long getDisorderMargin() {
        return disorderMargin;
    }

    public void setDisorderMargin(long disorderMargin) {
        this.disorderMargin = disorderMargin;
    }

    public String getEtcdEndpoints() {
        return etcdEndpoints;
    }

    public void setEtcdEndpoints(String etcdEndpoints) {
        this.etcdEndpoints = etcdEndpoints;
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

    public String getClients() {
        return clients;
    }

    public void setClients(String clients) {
        this.clients = clients;
    }

    public int getInstancesNumPerClient() {
        return instancesNumPerClient;
    }

    public void setInstancesNumPerClient(int instancesNumPerClient) {
        this.instancesNumPerClient = instancesNumPerClient;
    }

    public String getQueryOptimizer() {
        return queryOptimizer;
    }

    public void setQueryOptimizer(String queryOptimizer) {
        this.queryOptimizer = queryOptimizer;
    }

    public String getConstraintChecker() {
        return constraintChecker;
    }

    public void setConstraintChecker(String constraintChecker) {
        this.constraintChecker = constraintChecker;
    }

    public String getPhysicalOptimizer() {
        return physicalOptimizer;
    }

    public void setPhysicalOptimizer(String physicalOptimizer) {
        this.physicalOptimizer = physicalOptimizer;
    }

    public int getMemoryTaskThreadPoolSize() {
        return memoryTaskThreadPoolSize;
    }

    public void setMemoryTaskThreadPoolSize(int memoryTaskThreadPoolSize) {
        this.memoryTaskThreadPoolSize = memoryTaskThreadPoolSize;
    }

    public int getPhysicalTaskThreadPoolSizePerStorage() {
        return physicalTaskThreadPoolSizePerStorage;
    }

    public void setPhysicalTaskThreadPoolSizePerStorage(int physicalTaskThreadPoolSizePerStorage) {
        this.physicalTaskThreadPoolSizePerStorage = physicalTaskThreadPoolSizePerStorage;
    }

    public int getMaxCachedPhysicalTaskPerStorage() {
        return maxCachedPhysicalTaskPerStorage;
    }

    public void setMaxCachedPhysicalTaskPerStorage(int maxCachedPhysicalTaskPerStorage) {
        this.maxCachedPhysicalTaskPerStorage = maxCachedPhysicalTaskPerStorage;
    }

    public double getCachedTimeseriesProb() {
        return cachedTimeseriesProb;
    }

    public void setCachedTimeseriesProb(double cachedTimeseriesProb) {
        this.cachedTimeseriesProb = cachedTimeseriesProb;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getRetryWait() {
        return retryWait;
    }

    public void setRetryWait(int retryWait) {
        this.retryWait = retryWait;
    }

    public int getReAllocatePeriod() {
        return reAllocatePeriod;
    }

    public void setReAllocatePeriod(int reAllocatePeriod) {
        this.reAllocatePeriod = reAllocatePeriod;
    }

    public int getFragmentPerEngine() {
        return fragmentPerEngine;
    }

    public void setFragmentPerEngine(int fragmentPerEngine) {
        this.fragmentPerEngine = fragmentPerEngine;
    }

    public boolean isEnableStorageGroupValueLimit() {
        return enableStorageGroupValueLimit;
    }

    public void setEnableStorageGroupValueLimit(boolean enableStorageGroupValueLimit) {
        this.enableStorageGroupValueLimit = enableStorageGroupValueLimit;
    }

    public double getStorageGroupValueLimit() {
        return storageGroupValueLimit;
    }

    public void setStorageGroupValueLimit(double storageGroupValueLimit) {
        this.storageGroupValueLimit = storageGroupValueLimit;
    }
}
