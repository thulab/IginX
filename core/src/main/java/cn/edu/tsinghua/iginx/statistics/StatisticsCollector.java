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

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryResultCombineProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryResultCombineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class StatisticsCollector implements IStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsCollector.class);

    private final AtomicBoolean broadcast = new AtomicBoolean(false);

    private final ExecutorService broadcastThreadPool = Executors.newSingleThreadExecutor();

    private final PlanGenerateStatisticsCollector planGenerateStatisticsCollector = new PlanGenerateStatisticsCollector();

    private final QueryStatisticsCollector queryStatisticsCollector = new QueryStatisticsCollector();

    private final PlanExecuteStatisticsCollector planExecuteStatisticsCollector = new PlanExecuteStatisticsCollector();

    private final ResultCombineStatisticsCollector resultCombineStatisticsCollector = new ResultCombineStatisticsCollector();

    @Override
    public PostQueryExecuteProcessor getPostQueryExecuteProcessor() {
        return planExecuteStatisticsCollector.getPostQueryExecuteProcessor();
    }

    @Override
    public PostQueryPlanProcessor getPostQueryPlanProcessor() {
        return planGenerateStatisticsCollector.getPostQueryPlanProcessor();
    }

    @Override
    public PostQueryProcessor getPostQueryProcessor() {
        return queryStatisticsCollector.getPostQueryProcessor();
    }

    @Override
    public PostQueryResultCombineProcessor getPostQueryResultCombineProcessor() {
        return resultCombineStatisticsCollector.getPostQueryResultCombineProcessor();
    }

    @Override
    public PreQueryExecuteProcessor getPreQueryExecuteProcessor() {
        return planExecuteStatisticsCollector.getPreQueryExecuteProcessor();
    }

    @Override
    public PreQueryPlanProcessor getPreQueryPlanProcessor() {
        return planGenerateStatisticsCollector.getPreQueryPlanProcessor();
    }

    @Override
    public PreQueryProcessor getPreQueryProcessor() {
        return queryStatisticsCollector.getPreQueryProcessor();
    }

    @Override
    public PreQueryResultCombineProcessor getPreQueryResultCombineProcessor() {
        return resultCombineStatisticsCollector.getPreQueryResultCombineProcessor();
    }

    @Override
    public void startBroadcasting() {
        broadcast.set(true);
        // 启动一个新线程，定期播报统计信息
        broadcastThreadPool.execute(() -> {
            try {
                while (broadcast.get()) {
                    planGenerateStatisticsCollector.broadcastStatistics();
                    planExecuteStatisticsCollector.broadcastStatistics();
                    resultCombineStatisticsCollector.broadcastStatistics();
                    queryStatisticsCollector.broadcastStatistics();
                    Thread.sleep(ConfigDescriptor.getInstance().getConfig().getStatisticsLogInterval()); // 每隔 10 秒播报一次统计信息
                }
            } catch (InterruptedException e) {
                logger.error("encounter error when broadcasting statistics: ", e);
            }
        });
    }

    @Override
    public void endBroadcasting() {
        broadcast.set(false);
    }
}
