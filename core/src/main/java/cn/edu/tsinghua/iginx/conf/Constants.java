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

public class Constants {

    public static final String DEFAULT_USERNAME = "root";

    public static final String DEFAULT_PASSWORD = "root";

    public static final int MAX_REDIRECT_TIME = 5;

    public static final String CONFIG_FILE = "conf/config.properties";

    public static final String IP = "ip";

    public static final String PORT = "port";

    public static final String USERNAME = "root";

    public static final String PASSWORD = "root";

    public static final String MAX_ASYNC_RETRY_TIMES = "max_async_retry_times";

    public static final String ASYNC_EXECUTE_THREAD_POOL_SIZE = "async_execute_thread_pool_size";

    public static final String SYNC_EXECUTE_THREAD_POOL_SIZE = "sync_execute_thread_pool_size";

    public static final String REPLICA_NUM = "replica_num";

    public static final String ENABLE_STATISTICS_COLLECTION = "enable_statistics_collection";

    public static final String STATISTICS_COLLECTOR_CLASS_NAME = "statistics_collector_class_name";

    public static final String STATISTICS_LOG_INTERVAL = "statistics_log_interval";

    public static final String META_STORAGE = "meta_storage";

    public static final String FILE_DATA_DIR = "file_data_dir";

    public static final String ZOOKEEPER_CONNECTION_STRING = "zookeeper_connection_string";

    public static final String ETCD_ENDPOINTS = "etcd_endpoints";

    public static final String POLICY_CLASS_NAME = "policy_class_name";

    public static final String DISORDER_MARGIN = "disorder_margin";

    public static final String STORAGE_ENGINE_LIST = "storage_engine_list";

    public static final String DATABASE_CLASS_NAMES = "database_class_names";

    public static final String INFLUXDB_TOKEN = "influxDB_token";

    public static final String INFLUXDB_ORGANIZATION_NAME = "influxDB_organization_name";

    public static final String ENABLE_REST_SERVICE = "enable_rest_service";

    public static final String REST_IP = "rest_ip";

    public static final String REST_PORT = "rest_port";

    public static final String TIMESERIES_MAX_TAG_SIZE = "timeseries_max_tag_size";

    public static final String ENABLE_MQTT = "enable_mqtt";

    public static final String MQTT_HOST = "mqtt_host";

    public static final String MQTT_PORT = "mqtt_port";

    public static final String MQTT_HANDLER_POOL_SIZE = "mqtt_handler_pool_size";

    public static final String MQTT_PAYLOAD_FORMATTER = "mqtt_payload_formatter";

    public static final String MQTT_MAX_MESSAGE_SIZE = "mqtt_max_message_size";

}

