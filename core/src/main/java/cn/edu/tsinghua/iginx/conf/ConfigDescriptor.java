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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigDescriptor {

    private static final Logger logger = LoggerFactory.getLogger(ConfigDescriptor.class);

    private final Config config;

    private ConfigDescriptor() {
        config = new Config();
        loadProps();
    }

    private void loadProps() {
        File file = new File(Constants.CONFIG_FILE);
        logger.info(file.getAbsolutePath());
        try (InputStream in = new FileInputStream(Constants.CONFIG_FILE)) {
            Properties properties = new Properties();
            properties.load(in);

            config.setIp(properties.getProperty("ip", "0.0.0.0"));
            config.setPort(Integer.parseInt(properties.getProperty("port", "6324")));
            config.setUsername(properties.getProperty("username", "root"));
            config.setPassword(properties.getProperty("password", "root"));
            config.setZookeeperConnectionString(properties.getProperty("zookeeperConnectionString",
                    "127.0.0.1:2181"));
            config.setStorageEngineList(properties.getProperty("storageEngineList",
                    "127.0.0.1:8888:iotdb:username=root:password=root:readSessions=2:writeSessions=5,127.0.0.1:8889:iotdb:username=root:password=root:readSessions=2:writeSessions=5"));
            config.setMaxAsyncRetryTimes(Integer.parseInt(properties.getProperty("maxAsyncRetryTimes", "3")));
            config.setSyncExecuteThreadPool(Integer.parseInt(properties.getProperty("syncExecuteThreadPool", "60")));
            config.setAsyncExecuteThreadPool(Integer.parseInt(properties.getProperty("asyncExecuteThreadPool", "20")));
            config.setReplicaNum(Integer.parseInt(properties.getProperty("replicaNum", "1")));
            config.setDatabaseClassNames(properties.getProperty("databaseClassNames", "iotdb=cn.edu.tsinghua.iginx.iotdb.IoTDBPlanExecutor,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBPlanExecutor"));
            config.setPolicyClassName(properties.getProperty("policyClassName", "cn.edu.tsinghua.iginx.policy.NativePolicy"));
            config.setInfluxDBToken(properties.getProperty("influxDBToken", "your-token"));
            config.setInfluxDBOrganizationName(properties.getProperty("influxDBOrganizationName", "my-org"));

            config.setStatisticsCollectorClassName(properties.getProperty("statisticsCollectorClassName", ""));
            config.setStatisticsCollectorClassName(properties.getProperty("statisticsCollectorClassName", "cn.edu.tsinghua.iginx.statistics.StatisticsCollector"));
            config.setRestip(properties.getProperty("restip", "127.0.0.1"));
            config.setRestport(Integer.parseInt(properties.getProperty("restport", "6666")));


        } catch (IOException e) {
            logger.error("Fail to load properties: ", e);
        }
    }

    public static ConfigDescriptor getInstance() {
        return ConfigDescriptorHolder.INSTANCE;
    }

    public Config getConfig() {
        return config;
    }

    private static class ConfigDescriptorHolder {
        private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
    }

}
