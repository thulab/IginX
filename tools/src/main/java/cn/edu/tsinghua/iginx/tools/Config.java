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
package cn.edu.tsinghua.iginx.tools;

public class Config {

    private String ip = "0.0.0.0";

    private int port = 6888;

    private String username = "root";

    private String password = "root";

    private String MachineList = "1701,1702,1703";

    private String MetricList = "ZT13308,ZT13368,ZT13317,CY1";

    private String StartTime = "2021-11-16T00:00:00";

    private String EndTime = "2021-11-18T00:30:00";

    private String ExportFileDir = "/home";


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

    public String getExportFileDir() {
        return ExportFileDir;
    }

    public void setExportFileDir(String exportFileDir) {
        ExportFileDir = exportFileDir;
    }

    public String getEndTime() {
        return EndTime;
    }

    public void setEndTime(String endTime) {
        EndTime = endTime;
    }

    public String getStartTime() {
        return StartTime;
    }

    public void setStartTime(String startTime) {
        StartTime = startTime;
    }

    public String getMetricList() {
        return MetricList;
    }

    public void setMetricList(String metricList) {
        MetricList = metricList;
    }

    public String getMachineList() {
        return MachineList;
    }

    public void setMachineList(String machineList) {
        MachineList = machineList;
    }
}
