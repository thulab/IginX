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

import cn.edu.tsinghua.iginx.core.processor.PostQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryPlanProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PlanGenerateStatisticsCollector extends AbstractStageStatisticsCollector implements IPlanGenerateStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(PlanGenerateStatisticsCollector.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long count = 0;
    private long span = 0;

    @Override
    protected String getStageName() {
        return "PlanGenerate";
    }

    @Override
    protected void processStatistics(Statistics statistics) {
        lock.writeLock().lock();
        count += 1;
        span += statistics.getEndTime() - statistics.getBeginTime();
        lock.writeLock().unlock();
    }

    @Override
    public void broadcastStatistics() {
        lock.readLock().lock();
        logger.info("Plan Generate statisticsInfo: ");
        logger.info("\tcount: " + count + ", span: " + span + "μs");
        if (count != 0) {
            logger.info("\taverage-span: " + (1.0 * span) / count + "μs");
        }
        lock.readLock().unlock();
    }

    @Override
    public PostQueryPlanProcessor getPostQueryPlanProcessor() {
        return after::apply;
    }

    @Override
    public PreQueryPlanProcessor getPreQueryPlanProcessor() {
        return before::apply;
    }
}
