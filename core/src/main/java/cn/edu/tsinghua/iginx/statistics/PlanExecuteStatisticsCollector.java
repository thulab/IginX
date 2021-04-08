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
package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.core.processor.PostQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PlanExecuteStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(PlanExecuteStatisticsCollector.class);

    private final LinkedBlockingQueue<Statistics> statisticQueue = new LinkedBlockingQueue<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private long times = 0;

    private long totalSpan = 0;

    private long recentTimes = 0;

    private long recentSpan = 0;

    public PlanExecuteStatisticsCollector() {
        Executors.newSingleThreadExecutor()
                .submit(() -> {
                   while (true) {
                       Statistics statistics = statisticQueue.take();
                       long span = statistics.getEndTime() - statistics.getBeginTime();
                       lock.writeLock().lock();
                       recentTimes += 1;
                       recentSpan += span;
                       if (recentTimes % 1000 == 0) {
                           times += recentTimes;
                           totalSpan += recentSpan;
                           recentTimes = 0;
                           recentSpan = 0;
                       }
                       lock.writeLock().unlock();
                   }
                });
    }


    public void broadcastStatistics() {
        lock.readLock().lock();
        long times = this.times + this.recentTimes, totalSpan = this.totalSpan + this.recentSpan;
        logger.info("total-counts: " + times + ", total-span: " + totalSpan);
        if (times != 0) {
            logger.info("total-average-span: " + (1.0 * totalSpan) / times);
        }
        long recentTimes = this.recentTimes, recentSpan = this.recentSpan;
        logger.info("recent-counts: " + recentTimes + ", recent-span: " + recentSpan);
        if (recentTimes != 0) {
            logger.info("recent-average-span: " + (1.0 * recentSpan) / recentTimes);
        }
        lock.readLock().unlock();
    }

    public PostQueryExecuteProcessor getPostQueryExecuteProcessor() {
        return requestContext -> {
            long endExecuteTime = TimeUtils.getMicrosecond();
            long beginExecuteTime = (long) requestContext.getExtraParam("beginExecuteTime");
            statisticQueue.add(new Statistics(requestContext.getId(), beginExecuteTime, endExecuteTime, requestContext.getType()));
            return null;
        };
    }

    public PreQueryExecuteProcessor getPreQueryExecuteProcessor() {
        return requestContext -> {
            requestContext.setExtraParam("beginExecuteTime", TimeUtils.getMicrosecond());
            return null;
        };
    }

}
