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

import cn.edu.tsinghua.iginx.core.context.ContextType;
import cn.edu.tsinghua.iginx.core.processor.PostQueryResultCombineProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryResultCombineProcessor;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ResultCombineStatisticsCollector extends AbstractStageStatisticsCollector implements IPlanResultCombineStatisticsCollector  {

    private static final Logger logger = LoggerFactory.getLogger(ResultCombineStatisticsCollector.class);

    private long count = 0;

    private long span = 0;

    private final Map<ContextType, Pair<Long, Long>> detailInfos = new HashMap<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    protected String getStageName() {
        return "CombineResult";
    }

    @Override
    protected void processStatistics(Statistics statistics) {
        lock.writeLock().lock();
        count += 1;
        span += statistics.getEndTime() - statistics.getBeginTime();
        Pair<Long, Long> detailInfo = detailInfos.computeIfAbsent(statistics.getRequestContext().getType(), e -> new Pair<>(0L, 0L));
        detailInfo.k += 1;
        detailInfo.v += statistics.getEndTime() - statistics.getBeginTime();
        lock.writeLock().unlock();
    }

    @Override
    public void broadcastStatistics() {
        lock.readLock().lock();
        logger.info("Plan CombineResult statisticsInfo: ");
        logger.info("\tcount: " + count + ", span: " + span + "μs");
        if (count != 0) {
            logger.info("\taverage-span: " + (1.0 * span) / count + "μs");
        }
        for (Map.Entry<ContextType, Pair<Long, Long>> entry: detailInfos.entrySet()) {
            logger.info("\t\tFor Request: " + entry.getKey() + ", count: " + entry.getValue().k + ", span: " + entry.getValue().v + "μs");
        }
        lock.readLock().unlock();
    }

    @Override
    public PostQueryResultCombineProcessor getPostQueryResultCombineProcessor() {
        return after::apply;
    }

    @Override
    public PreQueryResultCombineProcessor getPreQueryResultCombineProcessor() {
        return before::apply;
    }
}
