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
package cn.edu.tsinghua.iginx.resource;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.resource.system.DefaultSystemMetricsService;
import cn.edu.tsinghua.iginx.resource.system.SystemMetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceManager {

    private static final Logger logger = LoggerFactory.getLogger(ResourceManager.class);

    private final SystemMetricsService systemMetrics;

    private final double heapMemoryThreshold;

    private final double systemMemoryThreshold;

    private final double systemCpuThreshold;

    private ResourceManager() {
        Config config = ConfigDescriptor.getInstance().getConfig();
        switch (config.getSystemResourceMetrics()) {
            case "default":
                systemMetrics = new DefaultSystemMetricsService();
                break;
            default:
                logger.info("use DefaultSystemMetrics as default");
                systemMetrics = new DefaultSystemMetricsService();
                break;
        }
        heapMemoryThreshold = config.getHeapMemoryThreshold();
        systemMemoryThreshold = config.getSystemMemoryThreshold();
        systemCpuThreshold = config.getSystemCpuThreshold();
    }

    public boolean reject(RequestContext ctx) {
        return heapMemoryOverwhelmed() || systemMetrics.getRecentCpuUsage() > systemCpuThreshold ||
                systemMetrics.getRecentMemoryUsage() > systemMemoryThreshold;
    }

    private boolean heapMemoryOverwhelmed() {
        return Runtime.getRuntime().totalMemory() * heapMemoryThreshold > Runtime.getRuntime().maxMemory();
    }

    public static ResourceManager getInstance() {
        return ResourceManagerHolder.INSTANCE;
    }

    private static class ResourceManagerHolder {

        private static final ResourceManager INSTANCE = new ResourceManager();

    }

}
