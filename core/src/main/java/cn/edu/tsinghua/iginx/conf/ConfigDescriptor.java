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

            config.setIp(
                    properties.getProperty(Constants.IP, config.getIp())
            );
            config.setPort(
                    Integer.parseInt(
                            properties.getProperty(Constants.PORT, Integer.toString(config.getPort()))
                    )
            );
            config.setUsername(
                    properties.getProperty(Constants.USERNAME, config.getIp())
            );
            config.setPassword(
                    properties.getProperty(Constants.PASSWORD, Integer.toString(config.getPort()))
            );
            config.setMaxAsyncRetryTimes(
                    Integer.parseInt(
                            properties.getProperty(Constants.MAX_ASYNC_RETRY_TIMES, Integer.toString(config.getMaxAsyncRetryTimes()))
                    )
            );
            config.setAsyncExecuteThreadPoolSize(
                    Integer.parseInt(
                            properties.getProperty(Constants.ASYNC_EXECUTE_THREAD_POOL_SIZE, Integer.toString(config.getAsyncExecuteThreadPoolSize()))
                    )
            );
            config.setSyncExecuteThreadPoolSize(
                    Integer.parseInt(
                            properties.getProperty(Constants.SYNC_EXECUTE_THREAD_POOL_SIZE, Integer.toString(config.getSyncExecuteThreadPoolSize()))
                    )
            );
            config.setReplicaNum(
                    Integer.parseInt(
                            properties.getProperty(Constants.REPLICA_NUM, Integer.toString(config.getReplicaNum()))
                    )
            );
            config.setEnableStatisticsCollection(
                    Boolean.parseBoolean(
                            properties.getProperty(Constants.ENABLE_STATISTICS_COLLECTION, Boolean.toString(config.isEnableStatisticsCollection()))
                    )
            );
            config.setStatisticsCollectorClassName(
                    properties.getProperty(Constants.STATISTICS_COLLECTOR_CLASS_NAME, config.getStatisticsCollectorClassName())
            );
            config.setStatisticsLogInterval(
                    Integer.parseInt(
                            properties.getProperty(Constants.STATISTICS_LOG_INTERVAL, "1000")
                    )
            );
            config.setMetaStorage(
                    properties.getProperty(Constants.META_STORAGE, config.getMetaStorage())
            );
            config.setZookeeperConnectionString(
                    properties.getProperty(Constants.ZOOKEEPER_CONNECTION_STRING, config.getZookeeperConnectionString())
            );
            config.setFileDataDir(
                    properties.getProperty(Constants.FILE_DATA_DIR, config.getFileDataDir())
            );
            config.setEtcdEndpoints(
                    properties.getProperty(Constants.ETCD_ENDPOINTS, config.getEtcdEndpoints())
            );
            config.setPolicyClassName(
                    properties.getProperty(Constants.POLICY_CLASS_NAME, config.getPolicyClassName())
            );
            config.setDisorderMargin(
                    Long.parseLong(
                            properties.getProperty(Constants.DISORDER_MARGIN, Long.toString(config.getDisorderMargin()))
                    )
            );
            config.setStorageEngineList(
                    properties.getProperty(Constants.STORAGE_ENGINE_LIST, config.getStorageEngineList())
            );
            config.setDatabaseClassNames(
                    properties.getProperty(Constants.DATABASE_CLASS_NAMES, config.getDatabaseClassNames())
            );
            config.setInfluxDBToken(
                    properties.getProperty(Constants.INFLUXDB_TOKEN, config.getInfluxDBToken())
            );
            config.setInfluxDBOrganizationName(
                    properties.getProperty(Constants.INFLUXDB_ORGANIZATION_NAME, config.getInfluxDBOrganizationName())
            );
            config.setEnableRestService(
                    Boolean.parseBoolean(
                            properties.getProperty(Constants.ENABLE_REST_SERVICE, Boolean.toString(config.isEnableRestService()))
                    )
            );
            config.setRestIp(
                    properties.getProperty(Constants.REST_IP, config.getRestIp())
            );
            config.setRestPort(
                    Integer.parseInt(
                            properties.getProperty(Constants.REST_PORT, Integer.toString(config.getRestPort()))
                    )
            );
            config.setTimeseriesMaxTagSize(
                    Integer.parseInt(
                            properties.getProperty(Constants.TIMESERIES_MAX_TAG_SIZE, Integer.toString(config.getTimeseriesMaxTagSize()))
                    )
            );
            config.setEnableMQTT(
                    Boolean.parseBoolean(
                            properties.getProperty(Constants.ENABLE_MQTT, Boolean.toString(config.isEnableMQTT()))
                    )
            );
            config.setMqttHost(
                    properties.getProperty(Constants.MQTT_HOST, config.getMqttHost())
            );
            config.setMqttPort(
                    Integer.parseInt(
                            properties.getProperty(Constants.MQTT_PORT, Integer.toString(config.getMqttPort()))
                    )
            );
            config.setMqttHandlerPoolSize(
                    Integer.parseInt(
                            properties.getProperty(Constants.MQTT_HANDLER_POOL_SIZE, Integer.toString(config.getMqttHandlerPoolSize()))
                    )
            );
            config.setMqttPayloadFormatter(
                    properties.getProperty(Constants.MQTT_PAYLOAD_FORMATTER, config.getMqttPayloadFormatter())
            );
            config.setMqttMaxMessageSize(
                    Integer.parseInt(
                            properties.getProperty(Constants.MQTT_MAX_MESSAGE_SIZE, Integer.toString(config.getMqttMaxMessageSize()))
                    )
            );
        } catch (IOException e) {
            logger.error("Fail to load properties: ", e);
        }
    }

    private void loadPropsFromEnv() {
        config.setIp(
                EnvUtils.loadEnv(Constants.IP, config.getIp())
        );
        config.setPort(
                EnvUtils.loadEnv(Constants.PORT, config.getPort())
        );
        config.setUsername(
                EnvUtils.loadEnv(Constants.USERNAME, config.getUsername())
        );
        config.setPassword(
                EnvUtils.loadEnv(Constants.PASSWORD, config.getPassword())
        );
        config.setMaxAsyncRetryTimes(
                EnvUtils.loadEnv(Constants.MAX_ASYNC_RETRY_TIMES, config.getMaxAsyncRetryTimes())
        );
        config.setAsyncExecuteThreadPoolSize(
                EnvUtils.loadEnv(Constants.ASYNC_EXECUTE_THREAD_POOL_SIZE, config.getAsyncExecuteThreadPoolSize())
        );
        config.setSyncExecuteThreadPoolSize(
                EnvUtils.loadEnv(Constants.SYNC_EXECUTE_THREAD_POOL_SIZE, config.getSyncExecuteThreadPoolSize())
        );
        config.setReplicaNum(
                EnvUtils.loadEnv(Constants.REPLICA_NUM, config.getReplicaNum())
        );
        config.setEnableStatisticsCollection(
                EnvUtils.loadEnv(Constants.ENABLE_STATISTICS_COLLECTION, config.isEnableStatisticsCollection())
        );
        config.setStatisticsCollectorClassName(
                EnvUtils.loadEnv(Constants.STATISTICS_COLLECTOR_CLASS_NAME, config.getStatisticsCollectorClassName())
        );
        config.setStatisticsLogInterval(
                EnvUtils.loadEnv(Constants.STATISTICS_LOG_INTERVAL, config.getStatisticsLogInterval())
        );
        config.setMetaStorage(
                EnvUtils.loadEnv(Constants.META_STORAGE, config.getMetaStorage())
        );
        config.setZookeeperConnectionString(
                EnvUtils.loadEnv(Constants.ZOOKEEPER_CONNECTION_STRING, config.getZookeeperConnectionString())
        );
        config.setFileDataDir(
                EnvUtils.loadEnv(Constants.FILE_DATA_DIR, config.getFileDataDir())
        );
        config.setEtcdEndpoints(
                EnvUtils.loadEnv(Constants.ETCD_ENDPOINTS, config.getEtcdEndpoints())
        );
        config.setPolicyClassName(
                EnvUtils.loadEnv(Constants.POLICY_CLASS_NAME, config.getPolicyClassName())
        );
        config.setDisorderMargin(
                EnvUtils.loadEnv(Constants.DISORDER_MARGIN, config.getDisorderMargin())
        );
        config.setStorageEngineList(
                EnvUtils.loadEnv(Constants.STORAGE_ENGINE_LIST, config.getStorageEngineList())
        );
        config.setDatabaseClassNames(
                EnvUtils.loadEnv(Constants.DATABASE_CLASS_NAMES, config.getDatabaseClassNames())
        );
        config.setInfluxDBToken(
                EnvUtils.loadEnv(Constants.INFLUXDB_TOKEN, config.getInfluxDBToken())
        );
        config.setInfluxDBOrganizationName(
                EnvUtils.loadEnv(Constants.INFLUXDB_ORGANIZATION_NAME, config.getInfluxDBOrganizationName())
        );
        config.setEnableRestService(
                EnvUtils.loadEnv(Constants.ENABLE_REST_SERVICE, config.isEnableRestService())
        );
        config.setRestIp(
                EnvUtils.loadEnv(Constants.REST_IP, config.getRestIp())
        );
        config.setRestPort(
                EnvUtils.loadEnv(Constants.REST_PORT, config.getRestPort())
        );
        config.setTimeseriesMaxTagSize(
                EnvUtils.loadEnv(Constants.TIMESERIES_MAX_TAG_SIZE, config.getTimeseriesMaxTagSize())
        );
        config.setEnableMQTT(
                EnvUtils.loadEnv(Constants.ENABLE_MQTT, config.isEnableMQTT())
        );
        config.setMqttHost(
                EnvUtils.loadEnv(Constants.MQTT_HOST, config.getMqttHost())
        );
        config.setMqttPort(
                EnvUtils.loadEnv(Constants.MQTT_PORT, config.getMqttPort())
        );
        config.setMqttHandlerPoolSize(
                EnvUtils.loadEnv(Constants.MQTT_HANDLER_POOL_SIZE, config.getMqttHandlerPoolSize())
        );
        config.setMqttPayloadFormatter(
                EnvUtils.loadEnv(Constants.MQTT_PAYLOAD_FORMATTER, config.getMqttPayloadFormatter())
        );
        config.setMqttMaxMessageSize(
                EnvUtils.loadEnv(Constants.MQTT_MAX_MESSAGE_SIZE, config.getMqttMaxMessageSize())
        );
    }


    public Config getConfig() {
        return config;
    }

    private static class ConfigDescriptorHolder {
        private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
    }

}
