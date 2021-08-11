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
package cn.edu.tsinghua.iginx.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class PayloadFormatManager {

    private static final Logger logger = LoggerFactory.getLogger(PayloadFormatManager.class);

    private static final PayloadFormatManager instance = new PayloadFormatManager();

    private final Map<String, IPayloadFormatter> formatters;

    private PayloadFormatManager() {
        this.formatters = new HashMap<>();
    }

    public static PayloadFormatManager getInstance() {
        return instance;
    }

    public IPayloadFormatter getFormatter(String formatterClassName) {
        IPayloadFormatter formatter;
        synchronized (formatters) {
            formatter = formatters.get(formatterClassName);
            if (formatter == null) {
                try {
                    Class<? extends IPayloadFormatter> clazz = (Class<? extends IPayloadFormatter>) this.getClass().getClassLoader().loadClass(formatterClassName);
                    formatter = clazz.getConstructor().newInstance();
                    formatters.put(formatterClassName, formatter);
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                    logger.error(e.getMessage());
                }
            }
        }
        return formatter;
    }

}
