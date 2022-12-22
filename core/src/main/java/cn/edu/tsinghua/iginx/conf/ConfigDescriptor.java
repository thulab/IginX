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

import java.io.*;
import java.util.Properties;

public class ConfigDescriptor {

    private static final Logger logger = LoggerFactory.getLogger(ConfigDescriptor.class);

    private final Config config;

    private ConfigDescriptor() {
        config = new Config();
        logger.info("load parameters from config.properties.");
        loadPropsFromFile();
        if (config.isEnableEnvParameter()) {
            logger.info("load parameters from env.");
            loadPropsFromEnv(); // 如果在环境变量中设置了相关参数，则会覆盖配置文件中设置的参数
        }
        if (config.isNeedInitBasicUDFFunctions()) {
            logger.info("load UDF list from file.");
            loadUDFListFromFile();
        }
    }

    public static ConfigDescriptor getInstance() {
        return ConfigDescriptorHolder.INSTANCE;
    }

    private void loadPropsFromFile() {
        try (InputStream in = new FileInputStream(EnvUtils.loadEnv(Constants.CONF, Constants.CONFIG_FILE))) {
            Properties properties = new Properties();
            properties.load(in);

            config.setIp(properties.getProperty("ip", "0.0.0.0"));
            config.setPort(Integer.parseInt(properties.getProperty("port", "6888")));
            config.setUsername(properties.getProperty("username", "root"));
            config.setPassword(properties.getProperty("password", "root"));
            config.setZookeeperConnectionString(properties.getProperty("zookeeperConnectionString",
                "127.0.0.1:2181"));
            config.setStorageEngineList(properties.getProperty("storageEngineList",
                "127.0.0.1#6667#iotdb11#username=root#password=root#sessionPoolSize=20#dataDir=/path/to/your/data/"));
            config.setMaxAsyncRetryTimes(Integer.parseInt(properties.getProperty("maxAsyncRetryTimes", "3")));
            config.setSyncExecuteThreadPool(Integer.parseInt(properties.getProperty("syncExecuteThreadPool", "60")));
            config.setAsyncExecuteThreadPool(Integer.parseInt(properties.getProperty("asyncExecuteThreadPool", "20")));
            config.setReplicaNum(Integer.parseInt(properties.getProperty("replicaNum", "1")));

            config.setDatabaseClassNames(properties.getProperty("databaseClassNames", "iotdb=cn.edu.tsinghua.iginx.iotdb.IoTDBPlanExecutor,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBPlanExecutor,parquet=cn.edu.tsinghua.iginx.parquet.parquetStorage"));
            //,opentsdb=cn.edu.tsinghua.iginx.opentsdb.OpenTSDBStorage,timescaledb=cn.edu.tsinghua.iginx.timescaledb.TimescaleDBStorage,postgresql=cn.edu.tsinghua.iginx.postgresql.PostgreSQLStorage

            config.setPolicyClassName(properties.getProperty("policyClassName", "cn.edu.tsinghua.iginx.policy.naive.NativePolicy"));
            config.setMigrationBatchSize(Integer.parseInt(properties.getProperty("migrationBatchSize", "100")));
            config.setReshardFragmentTimeMargin(Long.parseLong(properties.getProperty("reshardFragmentTimeMargin", "60")));
            config.setMaxReshardFragmentsNum(Integer.parseInt(properties.getProperty("maxReshardFragmentsNum", "3")));
            config.setMaxTimeseriesLoadBalanceThreshold(Double.parseDouble(properties.getProperty("maxTimeseriesLoadBalanceThreshold", "2")));
            config.setMigrationPolicyClassName(properties.getProperty("migrationPolicyClassName", "cn.edu.tsinghua.iginx.migration.SimulationBasedMigrationPolicy"));
            config.setEnableEnvParameter(Boolean.parseBoolean(properties.getProperty("enableEnvParameter", "false")));

            config.setStatisticsCollectorClassName(properties.getProperty("statisticsCollectorClassName", ""));
            config.setStatisticsLogInterval(Integer.parseInt(properties.getProperty("statisticsLogInterval", "5000")));

            config.setRestIp(properties.getProperty("restIp", "127.0.0.1"));
            config.setRestPort(Integer.parseInt(properties.getProperty("restPort", "6666")));

            config.setDisorderMargin(Long.parseLong(properties.getProperty("disorderMargin", "10")));
            config.setAsyncRestThreadPool(Integer.parseInt(properties.getProperty("asyncRestThreadPool", "100")));

            config.setMaxTimeseriesLength(Integer.parseInt(properties.getProperty("maxtimeserieslength", "10")));
            config.setEnableRestService(Boolean.parseBoolean(properties.getProperty("enableRestService", "true")));

            config.setMetaStorage(properties.getProperty("metaStorage", "zookeeper"));
            config.setEtcdEndpoints(properties.getProperty("etcdEndpoints", "http://localhost:2379"));

            config.setEnableMQTT(Boolean.parseBoolean(properties.getProperty("enable_mqtt", "false")));
            config.setMqttHost(properties.getProperty("mqtt_host", "0.0.0.0"));
            config.setMqttPort(Integer.parseInt(properties.getProperty("mqtt_port", "1883")));
            config.setMqttHandlerPoolSize(Integer.parseInt(properties.getProperty("mqtt_handler_pool_size", "1")));
            config.setMqttPayloadFormatter(properties.getProperty("mqtt_payload_formatter", "cn.edu.tsinghua.iginx.mqtt.JsonPayloadFormatter"));
            config.setMqttMaxMessageSize(Integer.parseInt(properties.getProperty("mqtt_max_message_size", "1048576")));

            config.setClients(properties.getProperty("clients", ""));
            config.setInstancesNumPerClient(Integer.parseInt(properties.getProperty("instancesNumPerClient", "0")));

            config.setQueryOptimizer(properties.getProperty("queryOptimizer", ""));
            config.setConstraintChecker(properties.getProperty("constraintChecker", "naive"));

            config.setPhysicalOptimizer(properties.getProperty("physicalOptimizer", "naive"));
            config.setMemoryTaskThreadPoolSize(Integer.parseInt(properties.getProperty("memoryTaskThreadPoolSize", "200")));
            config.setPhysicalTaskThreadPoolSizePerStorage(Integer.parseInt(properties.getProperty("physicalTaskThreadPoolSizePerStorage", "100")));

            config.setMaxCachedPhysicalTaskPerStorage(Integer.parseInt(properties.getProperty("maxCachedPhysicalTaskPerStorage", "500")));

            config.setCachedTimeseriesProb(Double.parseDouble(properties.getProperty("cachedTimeseriesProb", "0.01")));
            config.setRetryCount(Integer.parseInt(properties.getProperty("retryCount", "10")));
            config.setRetryWait(Integer.parseInt(properties.getProperty("retryWait", "5000")));
            config.setFragmentPerEngine(Integer.parseInt(properties.getProperty("fragmentPerEngine", "10")));
            config.setReAllocatePeriod(Integer.parseInt(properties.getProperty("reAllocatePeriod", "30000")));
            config.setEnableStorageGroupValueLimit(Boolean.parseBoolean(properties.getProperty("enableStorageGroupValueLimit", "true")));
            config.setStorageGroupValueLimit(Double.parseDouble(properties.getProperty("storageGroupValueLimit", "200.0")));

            config.setEnablePushDown(Boolean.parseBoolean(properties.getProperty("enablePushDown", "true")));
            config.setUseStreamExecutor(Boolean.parseBoolean(properties.getProperty("useStreamExecutor", "true")));

            config.setEnableMemoryControl(Boolean.parseBoolean(properties.getProperty("enable_memory_control", "true")));
            config.setSystemResourceMetrics(properties.getProperty("system_resource_metrics", "default"));
            config.setHeapMemoryThreshold(Double.parseDouble(properties.getProperty("heap_memory_threshold", "0.9")));
            config.setSystemMemoryThreshold(Double.parseDouble(properties.getProperty("system_memory_threshold", "0.9")));
            config.setSystemCpuThreshold(Double.parseDouble(properties.getProperty("system_cpu_threshold", "0.9")));

            config.setEnableMetaCacheControl(Boolean.parseBoolean(properties.getProperty("enable_meta_cache_control", "false")));
            config.setFragmentCacheThreshold(Long.parseLong(properties.getProperty("fragment_cache_threshold", "131072")));
            config.setBatchSize(Integer.parseInt(properties.getProperty("batchSize", "50")));
            config.setPythonCMD(properties.getProperty("pythonCMD", "python3"));
            config.setTransformTaskThreadPoolSize(Integer.parseInt(properties.getProperty("transformTaskThreadPoolSize", "10")));
            config.setTransformMaxRetryTimes(Integer.parseInt(properties.getProperty("transformMaxRetryTimes", "3")));

            config.setNeedInitBasicUDFFunctions(Boolean.parseBoolean(properties.getProperty("needInitBasicUDFFunctions", "false")));

            config.setHistoricalPrefixList(properties.getProperty("historicalPrefixList", ""));
            config.setExpectedStorageUnitNum(Integer.parseInt(properties.getProperty("expectedStorageUnitNum", "0")));
            config.setLocalParquetStorage(Boolean.parseBoolean(properties.getProperty("isLocalParquetStorage", "true")));

            // 容错相关
            config.setStorageHeartbeatInterval(ConfigUtils.parseTime(properties.getProperty("storage_heartbeat_interval", "10s")));
            config.setStorageHeartbeatMaxRetryTimes(Integer.parseInt(properties.getProperty("storage_heartbeat_max_retry_times", "5")));
            config.setStorageHeartbeatTimeout(ConfigUtils.parseTime(properties.getProperty("storage_heartbeat_timeout", "1s")));
            config.setStorageRetryConnectInterval(ConfigUtils.parseTime(properties.getProperty("storage_retry_connect_interval", "50s")));
            config.setStorageHeartbeatThresholdPoolSize(Integer.parseInt(properties.getProperty("storage_heartbeat_threshold_pool_size", "10")));
            config.setStorageRestoreHeartbeatProbability(Double.parseDouble(properties.getProperty("storage_restore_heartbeat_probability", "0.05")));
            config.setMigrationThreadPoolSize(Integer.parseInt(properties.getProperty("migration_thread_pool_size", "20")));
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
        config.setStatisticsCollectorClassName(EnvUtils.loadEnv("statisticsCollectorClassName", config.getStatisticsCollectorClassName()));
        config.setStatisticsLogInterval(EnvUtils.loadEnv("statisticsLogInterval", config.getStatisticsLogInterval()));
        config.setRestIp(EnvUtils.loadEnv("restIp", config.getRestIp()));
        config.setRestPort(EnvUtils.loadEnv("restPort", config.getRestPort()));
        config.setDisorderMargin(EnvUtils.loadEnv("disorderMargin", config.getDisorderMargin()));
        config.setMaxTimeseriesLength(EnvUtils.loadEnv("maxtimeserieslength", config.getMaxTimeseriesLength()));
        config.setAsyncRestThreadPool(EnvUtils.loadEnv("asyncRestThreadPool", config.getAsyncRestThreadPool()));
        config.setEnableRestService(EnvUtils.loadEnv("enableRestService", config.isEnableRestService()));
        config.setMetaStorage(EnvUtils.loadEnv("metaStorage", config.getMetaStorage()));
        config.setEtcdEndpoints(EnvUtils.loadEnv("etcdEndpoints", config.getEtcdEndpoints()));
        config.setEnableMQTT(EnvUtils.loadEnv("enable_mqtt", config.isEnableMQTT()));
        config.setMqttHost(EnvUtils.loadEnv("mqtt_host", config.getMqttHost()));
        config.setMqttPort(EnvUtils.loadEnv("mqtt_port", config.getMqttPort()));
        config.setMqttHandlerPoolSize(EnvUtils.loadEnv("mqtt_handler_pool_size", config.getMqttHandlerPoolSize()));
        config.setMqttPayloadFormatter(EnvUtils.loadEnv("mqtt_payload_formatter", config.getMqttPayloadFormatter()));
        config.setMqttMaxMessageSize(EnvUtils.loadEnv("mqtt_max_message_size", config.getMqttMaxMessageSize()));
        config.setQueryOptimizer(EnvUtils.loadEnv("queryOptimizer", config.getQueryOptimizer()));
        config.setConstraintChecker(EnvUtils.loadEnv("constraintChecker", config.getConstraintChecker()));
        config.setPhysicalOptimizer(EnvUtils.loadEnv("physicalOptimizer", config.getPhysicalOptimizer()));
        config.setMemoryTaskThreadPoolSize(EnvUtils.loadEnv("memoryTaskThreadPoolSize", config.getMemoryTaskThreadPoolSize()));
        config.setPhysicalTaskThreadPoolSizePerStorage(EnvUtils.loadEnv("physicalTaskThreadPoolSizePerStorage", config.getPhysicalTaskThreadPoolSizePerStorage()));
        config.setMaxCachedPhysicalTaskPerStorage(EnvUtils.loadEnv("maxCachedPhysicalTaskPerStorage", config.getMaxCachedPhysicalTaskPerStorage()));
        config.setCachedTimeseriesProb(EnvUtils.loadEnv("cachedTimeseriesProb", config.getCachedTimeseriesProb()));
        config.setRetryCount(EnvUtils.loadEnv("retryCount", config.getRetryCount()));
        config.setRetryWait(EnvUtils.loadEnv("retryWait", config.getRetryWait()));
        config.setFragmentPerEngine(EnvUtils.loadEnv("fragmentPerEngine", config.getFragmentPerEngine()));
        config.setReAllocatePeriod(EnvUtils.loadEnv("reAllocatePeriod", config.getReAllocatePeriod()));
        config.setEnableStorageGroupValueLimit(EnvUtils.loadEnv("enableStorageGroupValueLimit", config.isEnableStorageGroupValueLimit()));
        config.setStorageGroupValueLimit(EnvUtils.loadEnv("storageGroupValueLimit", config.getStorageGroupValueLimit()));
        config.setEnablePushDown(EnvUtils.loadEnv("enablePushDown", config.isEnablePushDown()));
        config.setUseStreamExecutor(EnvUtils.loadEnv("useStreamExecutor", config.isUseStreamExecutor()));
        config.setEnableMemoryControl(EnvUtils.loadEnv("enable_memory_control", config.isEnableMemoryControl()));
        config.setSystemResourceMetrics(EnvUtils.loadEnv("system_resource_metrics", config.getSystemResourceMetrics()));
        config.setHeapMemoryThreshold(EnvUtils.loadEnv("heap_memory_threshold", config.getHeapMemoryThreshold()));
        config.setSystemMemoryThreshold(EnvUtils.loadEnv("system_memory_threshold", config.getSystemMemoryThreshold()));
        config.setSystemCpuThreshold(EnvUtils.loadEnv("system_cpu_threshold", config.getSystemCpuThreshold()));
        config.setEnableMetaCacheControl(EnvUtils.loadEnv("enable_meta_cache_control", config.isEnableMetaCacheControl()));
        config.setFragmentCacheThreshold(EnvUtils.loadEnv("fragment_cache_threshold", config.getFragmentCacheThreshold()));
        config.setBatchSize(EnvUtils.loadEnv("batchSize", config.getBatchSize()));
        config.setPythonCMD(EnvUtils.loadEnv("pythonCMD", config.getPythonCMD()));
        config.setTransformTaskThreadPoolSize(EnvUtils.loadEnv("transformTaskThreadPoolSize", config.getTransformTaskThreadPoolSize()));
        config.setTransformMaxRetryTimes(EnvUtils.loadEnv("transformMaxRetryTimes", config.getTransformMaxRetryTimes()));
        config.setNeedInitBasicUDFFunctions(EnvUtils.loadEnv("needInitBasicUDFFunctions", config.isNeedInitBasicUDFFunctions()));
        config.setHistoricalPrefixList(EnvUtils.loadEnv("historicalPrefixList", config.getHistoricalPrefixList()));
        config.setExpectedStorageUnitNum(EnvUtils.loadEnv("expectedStorageUnitNum", config.getExpectedStorageUnitNum()));
        config.setLocalParquetStorage(EnvUtils.loadEnv("isLocalParquetStorage", config.isLocalParquetStorage()));

        // 容错相关
        config.setStorageHeartbeatInterval(ConfigUtils.parseTime(EnvUtils.loadEnv("storage_heartbeat_interval", ConfigUtils.toTimeString(config.getStorageHeartbeatInterval()))));
        config.setStorageHeartbeatMaxRetryTimes(EnvUtils.loadEnv("storage_heartbeat_max_retry_times", config.getStorageHeartbeatMaxRetryTimes()));
        config.setStorageHeartbeatTimeout(ConfigUtils.parseTime(EnvUtils.loadEnv("storage_heartbeat_timeout", ConfigUtils.toTimeString(config.getStorageHeartbeatTimeout()))));
        config.setStorageRetryConnectInterval(ConfigUtils.parseTime(EnvUtils.loadEnv("storage_retry_connect_interval", ConfigUtils.toTimeString(config.getStorageRetryConnectInterval()))));
        config.setStorageHeartbeatThresholdPoolSize(EnvUtils.loadEnv("storageHeartbeatThresholdPoolSize", config.getStorageHeartbeatThresholdPoolSize()));
        config.setStorageRestoreHeartbeatProbability(EnvUtils.loadEnv("storageRestoreHeartbeatProbability", config.getStorageRestoreHeartbeatProbability()));
    }

    private void loadUDFListFromFile() {
        try (InputStream in = new FileInputStream(EnvUtils.loadEnv(Constants.UDF_LIST, Constants.UDF_LIST_FILE))) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));

            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                line = line.trim();
                if (line.toLowerCase().startsWith(Constants.UDAF) ||
                    line.toLowerCase().startsWith(Constants.UDTF) ||
                    line.toLowerCase().startsWith(Constants.UDSF) ||
                    line.toLowerCase().startsWith(Constants.TRANSFORM)) {
                    config.getUdfList().add(line);
                }
            }
        } catch (IOException e) {
            logger.error("Fail to load udf list: ", e);
        }
    }

    public Config getConfig() {
        return config;
    }

    private static class ConfigDescriptorHolder {
        private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
    }

}
