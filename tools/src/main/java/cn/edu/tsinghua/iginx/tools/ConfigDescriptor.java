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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigDescriptor {

    private static final Logger logger = LoggerFactory.getLogger(ConfigDescriptor.class);

    private final Config config;

    private ConfigDescriptor() {
        config = new Config();
        logger.info("load parameters from config.properties.");
        loadPropsFromFile();
    }

    public static ConfigDescriptor getInstance() {
        return ConfigDescriptorHolder.INSTANCE;
    }

    private void loadPropsFromFile() {
        try (InputStream in = new FileInputStream(loadEnv(Constants.CONF, Constants.CONFIG_FILE))) {
            Properties properties = new Properties();
            properties.load(in);

            config.setIp(properties.getProperty("ip", "0.0.0.0"));
            config.setPort(Integer.parseInt(properties.getProperty("port", "6888")));
            config.setUsername(properties.getProperty("username", "root"));
            config.setPassword(properties.getProperty("password", "root"));

            config.setMachineList(properties.getProperty("MachineList", "1701,1702,1703"));
            config.setMetricList(properties.getProperty("MetricList", "ZT13308,ZT13368,ZT13317,CY1"));
            config.setStartTime(properties.getProperty("StartTime", "2021-11-16T00:00:00"));
            config.setEndTime(properties.getProperty("EndTime", "2021-11-18T00:30:00"));
            config.setExportFileDir(properties.getProperty("ExportFileDir", "/home"));

        } catch (IOException e) {
            logger.error("Fail to load properties: ", e);
        }
    }



    public Config getConfig() {
        return config;
    }

    private static class ConfigDescriptorHolder {
        private static final ConfigDescriptor INSTANCE = new ConfigDescriptor();
    }

    public static String loadEnv(String name, String defaultValue) {
        String env = System.getenv(name);
        env = env==null?System.getProperty(name):env;
        if (env == null) {
            return defaultValue;
        }
        return env;
    }
}
