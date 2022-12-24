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
package cn.edu.tsinghua.iginx.resource.system;

import cn.hutool.system.oshi.OshiUtil;
import com.google.common.util.concurrent.AtomicDouble;
import oshi.hardware.GlobalMemory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultSystemMetricsService implements SystemMetricsService {

    private static final int SAMPLE_SIZE = 60;

    private static final int UPDATE_PER_SAMPLE = 10; // UPDATE_PER_SAMPLE 必须能整除 SAMPLE_SIZE

    private static final long STATISTICS_INTERVAL = 1000L;

    private final double[] cpuUsages;

    private final double[] memoryUsage;

    private final AtomicDouble recentCpuUsage;

    private final AtomicDouble recentMemoryUsage;

    private final ExecutorService exec;

    private int index = 0;

    public DefaultSystemMetricsService() {
        recentCpuUsage = new AtomicDouble(0.0);
        recentMemoryUsage = new AtomicDouble(0.0);
        cpuUsages = new double[SAMPLE_SIZE];
        memoryUsage = new double[SAMPLE_SIZE];
        exec = Executors.newSingleThreadExecutor();
    }

    @Override
    public void start() {
        exec.execute(() -> {
            cpuUsages[index] = (100.0 - OshiUtil.getCpuInfo(STATISTICS_INTERVAL).getFree()) / 100.0;
            GlobalMemory memory = OshiUtil.getMemory();
            memoryUsage[index] = (memory.getTotal() - memory.getAvailable()) * 1.0 / memory.getTotal();
            index++;
            if (index % UPDATE_PER_SAMPLE == 0) {
                recentCpuUsage.set(avg(cpuUsages));
                recentMemoryUsage.set(avg(memoryUsage));
            }
            index %= SAMPLE_SIZE;
        });
    }

    @Override
    public void stop() {
        exec.shutdown();
    }

    @Override
    public double getRecentCpuUsage() {
        return recentCpuUsage.get();
    }

    @Override
    public double getRecentMemoryUsage() {
        return recentMemoryUsage.get();
    }

    private static double avg(double[] arr) {
        if (arr == null || arr.length == 0) {
            return 0.0;
        }
        double sum = 0.0;
        for (double value: arr) {
            sum += value;
        }
        return sum / arr.length;
    }

}
