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

import cn.edu.tsinghua.iginx.combine.QueryDataCombineResult;
import cn.edu.tsinghua.iginx.core.context.ContextType;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.processor.PostQueryProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryProcessor;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedRowRecordsPlan;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryStatisticsCollector extends AbstractStageStatisticsCollector implements IQueryStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(QueryStatisticsCollector.class);
    private final Map<ContextType, Pair<Long, Long>> detailInfos = new HashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long count = 0;
    private long span = 0;
    private long queryPoints = 0;
    private long insertPoints = 0;

    @Override
    protected String getStageName() {
        return "Query";
    }

    @Override
    protected void processStatistics(Statistics statistics) {
        lock.writeLock().lock();
        count += 1;
        span += statistics.getEndTime() - statistics.getBeginTime();
        Pair<Long, Long> detailInfo = detailInfos.computeIfAbsent(statistics.getRequestContext().getType(), e -> new Pair<>(0L, 0L));
        detailInfo.k += 1;
        detailInfo.v += statistics.getEndTime() - statistics.getBeginTime();
        if (statistics.getRequestContext().getType() == ContextType.InsertNonAlignedRowRecords) {
            insertPoints += statistics.getRequestContext().getIginxPlans().stream().map(InsertNonAlignedRowRecordsPlan.class::cast).mapToInt(e -> e.getTimestamps().length * e.getPathsNum()).sum();
        }
        if (statistics.getRequestContext().getType() == ContextType.InsertNonAlignedColumnRecords) {
            insertPoints += statistics.getRequestContext().getIginxPlans().stream().map(InsertNonAlignedColumnRecordsPlan.class::cast).mapToInt(e -> e.getPathsNum() * e.getTimestamps().length).sum();
        }
        if (statistics.getRequestContext().getType() == ContextType.QueryData) {
            queryPoints += (long) ((QueryDataCombineResult) statistics.getRequestContext().getCombineResult()).getResp().queryDataSet.getBitmapListSize() * ((QueryDataContext) statistics.getRequestContext()).getReq().paths.size();
        }
        lock.writeLock().unlock();
    }

    @Override
    public void broadcastStatistics() {
        lock.readLock().lock();
        logger.info("Query statisticsInfo: ");
        logger.info("\tcount: " + count + ", span: " + span + "μs");
        if (count != 0) {
            logger.info("\taverage-span: " + (1.0 * span) / count + "μs");
        }
        for (Map.Entry<ContextType, Pair<Long, Long>> entry : detailInfos.entrySet()) {
            logger.info("\t\tFor Request: " + entry.getKey() + ", count: " + entry.getValue().k + ", span: " + entry.getValue().v + "μs");
        }
        logger.info("\ttotal insert points: " + insertPoints);
        logger.info("\ttotal query points: " + queryPoints);
        lock.readLock().unlock();
    }

    @Override
    public PreQueryProcessor getPreQueryProcessor() {
        return before::apply;
    }

    @Override
    public PostQueryProcessor getPostQueryProcessor() {
        return after::apply;
    }

}
