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
package cn.edu.tsinghua.iginx.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvUtils {

    private static final Logger logger = LoggerFactory.getLogger(EnvUtils.class);

    public static boolean loadEnv(String name, boolean defaultValue) {
        String env = System.getenv(name);
        env = env==null?System.getProperty(name):env;
        if (env == null) {
            return defaultValue;
        }
        try {
            return Boolean.parseBoolean(env);
        } catch (NumberFormatException e) {
            logger.error("unexpected boolean env: {} = {}", name, env);
            return defaultValue;
        }
    }

    public static long loadEnv(String name, long defaultValue) {
        String env = System.getenv(name);
        env = env==null?System.getProperty(name):env;
        if (env == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(env);
        } catch (NumberFormatException e) {
            logger.error("unexpected long env: {} = {}", name, env);
            return defaultValue;
        }
    }

    public static int loadEnv(String name, int defaultValue) {
        String env = System.getenv(name);
        env = env==null?System.getProperty(name):env;
        if (env == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(env);
        } catch (NumberFormatException e) {
            logger.error("unexpected int env: {} = {}", name, env);
            return defaultValue;
        }
    }

    public static double loadEnv(String name, double defaultValue) {
        String env = System.getenv(name);
        env = env==null?System.getProperty(name):env;
        if (env == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(env);
        } catch (NumberFormatException e) {
            logger.error("unexpected double env: {} = {}", name, env);
            return defaultValue;
        }
    }

    public static float loadEnv(String name, float defaultValue) {
        String env = System.getenv(name);
        env = env==null?System.getProperty(name):env;
        if (env == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(env);
        } catch (NumberFormatException e) {
            logger.error("unexpected float env: {} = {}", name, env);
            return defaultValue;
        }
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
