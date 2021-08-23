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

import cn.edu.tsinghua.iginx.utils.EnvUtils;
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
        loadPropsFromFile();
        loadPropsFromEnv(); // 如果在环境变量中设置了相关参数，则会覆盖配置文件中设置的参数
    }

    public static ConfigDescriptor getInstance() {
        return ConfigDescriptorHolder.INSTANCE;
    }

    private void loadPropsFromFile() {
        File file = new File(Constants.CONFIG_FILE);
        logger.info(file.getAbsolutePath());
        try (InputStream in = new FileInputStream(Constants.CONFIG_FILE)) {
            Properties properties = new Properties();
            properties.load(in);

            config.setIp(properties.getProperty("ip", "0.0.0.0"));
            config.setPort(Integer.parseInt(properties.getProperty("port", "6888")));
            config.setUsername(properties.getProperty("username", "root"));
            config.setPassword(properties.getProperty("password", "root"));
            config.setZookeeperConnectionString(properties.getProperty("zookeeperConnectionString",
                    "127.0.0.1:2181"));
            config.setStorageEngineList(properties.getProperty("storageEngineList",
                    "127.0.0.1:6667:iotdb:username=root:password=root:sessionPoolSize=100"));
            config.setMaxAsyncRetryTimes(Integer.parseInt(properties.getProperty("maxAsyncRetryTimes", "3")));
            config.setSyncExecuteThreadPool(Integer.parseInt(properties.getProperty("syncExecuteThreadPool", "60")));
            config.setAsyncExecuteThreadPool(Integer.parseInt(properties.getProperty("asyncExecuteThreadPool", "20")));
            config.setReplicaNum(Integer.parseInt(properties.getProperty("replicaNum", "1")));
            config.setDatabaseClassNames(properties.getProperty("databaseClassNames", "iotdb=cn.edu.tsinghua.iginx.iotdb.IoTDBPlanExecutor,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBPlanExecutor"));
            config.setPolicyClassName(properties.getProperty("policyClassName", "cn.edu.tsinghua.iginx.policy.naive.NativePolicy"));

            config.setStorageUnitNum(Integer.parseInt(properties.getProperty("storageUnitNum", "30")));

            config.setStatisticsCollectorClassName(properties.getProperty("statisticsCollectorClassName", ""));
            config.setStatisticsLogInterval(Integer.parseInt(properties.getProperty("statisticsLogInterval", "1000")));

            config.setRestIp(properties.getProperty("restIp", "127.0.0.1"));
            config.setRestPort(Integer.parseInt(properties.getProperty("restPort", "6666")));

            config.setDisorderMargin(Long.parseLong(properties.getProperty("disorderMargin", "10")));

            config.setMaxTimeseriesLength(Integer.parseInt(properties.getProperty("maxtimeserieslength", "10")));
            config.setEnableRestService(Boolean.parseBoolean(properties.getProperty("enableRestService", "true")));

            config.setMetaStorage(properties.getProperty("metaStorage", "zookeeper"));
            config.setFileDataDir(properties.getProperty("fileDataDir", ""));
            config.setEtcdEndpoints(properties.getProperty("etcdEndpoints", "http://localhost:2379"));

            config.setEnableMQTT(Boolean.parseBoolean(properties.getProperty("enable_mqtt", "false")));
            config.setMqttHost(properties.getProperty("mqtt_host", "0.0.0.0"));
            config.setMqttPort(Integer.parseInt(properties.getProperty("mqtt_port", "1883")));
            config.setMqttHandlerPoolSize(Integer.parseInt(properties.getProperty("mqtt_handler_pool_size", "1")));
            config.setMqttPayloadFormatter(properties.getProperty("mqtt_payload_formatter", "cn.edu.tsinghua.iginx.mqtt.JsonPayloadFormatter"));
            config.setMqttMaxMessageSize(Integer.parseInt(properties.getProperty("mqtt_max_message_size", "1048576")));

            config.setEnableEdgeCloudCollaboration(Boolean.parseBoolean(properties.getProperty("enable_edge_cloud_collaboration", "false")));
            config.setEdge(Boolean.parseBoolean(properties.getProperty("is_edge", "false")));
            config.setEdgeName(properties.getProperty("edge_name", ""));
        } catch (IOException e) {
            logger.error("Fail to load properties: ", e);
        }
    }

    private void loadPropsFromEnv() {
        config.setIp(EnvUtils.loadEnv("ip", config.getIp()));
        config.setPort(EnvUtils.loadEnv("port", config.getPort()));
        config.setUsername(EnvUtils.loadEnv("username", config.getUsername()));
        config.setPassword(EnvUtils.loadEnv("password", config.getPassword()));
        config.setZookeeperConnectionString(EnvUtils.loadEnv("zookeeperConnectionString", config.getZookeeperConnectionString()));
        config.setStorageEngineList(EnvUtils.loadEnv("storageEngineList", config.getStorageEngineList()));
        config.setMaxAsyncRetryTimes(EnvUtils.loadEnv("maxAsyncRetryTimes", config.getMaxAsyncRetryTimes()));
        config.setSyncExecuteThreadPool(EnvUtils.loadEnv("syncExecuteThreadPool", config.getSyncExecuteThreadPool()));
        config.setAsyncExecuteThreadPool(EnvUtils.loadEnv("asyncExecuteThreadPool", config.getAsyncExecuteThreadPool()));
        config.setReplicaNum(EnvUtils.loadEnv("replicaNum", config.getReplicaNum()));
        config.setDatabaseClassNames(EnvUtils.loadEnv("databaseClassNames", config.getDatabaseClassNames()));
        config.setPolicyClassName(EnvUtils.loadEnv("policyClassName", config.getPolicyClassName()));
        config.setStorageUnitNum(EnvUtils.loadEnv("storageUnitNum", config.getStorageUnitNum()));
        config.setStatisticsCollectorClassName(EnvUtils.loadEnv("statisticsCollectorClassName", config.getStatisticsCollectorClassName()));
        config.setStatisticsLogInterval(EnvUtils.loadEnv("statisticsLogInterval", config.getStatisticsLogInterval()));
        config.setRestIp(EnvUtils.loadEnv("restIp", config.getRestIp()));
        config.setRestPort(EnvUtils.loadEnv("restPort", config.getRestPort()));
        config.setDisorderMargin(EnvUtils.loadEnv("disorderMargin", config.getDisorderMargin()));
        config.setMaxTimeseriesLength(EnvUtils.loadEnv("maxtimeserieslength", config.getMaxTimeseriesLength()));
        config.setEnableRestService(EnvUtils.loadEnv("enableRestService", config.isEnableRestService()));
        config.setMetaStorage(EnvUtils.loadEnv("metaStorage", config.getMetaStorage()));
        config.setFileDataDir(EnvUtils.loadEnv("fileDataDir", config.getFileDataDir()));
        config.setEtcdEndpoints(EnvUtils.loadEnv("etcdEndpoints", config.getEtcdEndpoints()));
        config.setEnableMQTT(EnvUtils.loadEnv("enable_mqtt", config.isEnableMQTT()));
        config.setMqttHost(EnvUtils.loadEnv("mqtt_host", config.getMqttHost()));
        config.setMqttPort(EnvUtils.loadEnv("mqtt_port", config.getMqttPort()));
        config.setMqttHandlerPoolSize(EnvUtils.loadEnv("mqtt_handler_pool_size", config.getMqttHandlerPoolSize()));
        config.setMqttPayloadFormatter(EnvUtils.loadEnv("mqtt_payload_formatter", config.getMqttPayloadFormatter()));
        config.setMqttMaxMessageSize(EnvUtils.loadEnv("mqtt_max_message_size", config.getMqttMaxMessageSize()));
    }


    public Config getConfig() {
        return config;
    }

    private static class ConfigDescriptorHolder {
        private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
    }

}
