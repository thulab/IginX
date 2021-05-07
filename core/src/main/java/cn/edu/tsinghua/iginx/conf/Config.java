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

    private int port = 6324;

    private String username = "root";

    private String password = "root";

    private String zookeeperConnectionString = "127.0.0.1:2181";

    private String storageEngineList = "127.0.0.1:8888:iotdb:username=root:password=root:readSessions=2:writeSessions=5,127.0.0.1:8889:iotdb:username=root:password=root:readSessions=2:writeSessions=5";

    private int maxAsyncRetryTimes = 2;

    private int syncExecuteThreadPool = 60;

    private int asyncExecuteThreadPool = 20;

    private int replicaNum = 1;

    private String databaseClassNames = "iotdb=cn.edu.tsinghua.iginx.iotdb.IoTDBPlanExecutor,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBPlanExecutor";

    private String policyClassName = "cn.edu.tsinghua.iginx.policy.NativePolicy";

    private String influxDBToken = "token";

    private String influxDBOrganizationName = "organization";

    private String statisticsCollectorClassName = "cn.edu.tsinghua.iginx.statistics.StatisticsCollector";

    private String restip = "127.0.0.1";

    private int restport = 6666;

    private int defaultTimeseriesLength = 2;

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

    public String getStatisticsCollectorClassName() {
        return statisticsCollectorClassName;
    }

    public void setStatisticsCollectorClassName(String statisticsCollectorClassName) {
        this.statisticsCollectorClassName = statisticsCollectorClassName;
    }

    public String getRestip()
    {
        return restip;
    }

    public int getRestport()
    {
        return restport;
    }

    public int getDefaultTimeseriesLength()
    {
        return defaultTimeseriesLength;
    }

    public void setDefaultTimeseriesLength(int defaultTimeseriesLength)
    {
        this.defaultTimeseriesLength = defaultTimeseriesLength;
    }
}
