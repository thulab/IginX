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

import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.TimeUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

public abstract class AbstractStageStatisticsCollector {

    protected static final String BEGIN = "begin";

    protected static final String END = "end";
    private final LinkedBlockingQueue<Statistics> statisticsQueue = new LinkedBlockingQueue<>();
    protected Function<RequestContext, Status> before = requestContext -> {
        requestContext.setExtraParam(BEGIN + getStageName(), TimeUtils.getMicrosecond());
        return null;
    };
    protected Function<RequestContext, Status> after = requestContext -> {
        long endTime = TimeUtils.getMicrosecond();
        requestContext.setExtraParam(END + getStageName(), endTime);
        long beginTime = (long) requestContext.getExtraParam(BEGIN + getStageName());
        statisticsQueue.add(new Statistics(requestContext.getId(), beginTime, endTime, requestContext));
        return null;
    };

    public AbstractStageStatisticsCollector() {
        Executors.newSingleThreadExecutor()
                .submit(() -> {
                    while (true) {
                        Statistics statistics = statisticsQueue.take();
                        processStatistics(statistics);
                    }
                });
    }

    protected abstract String getStageName();

    protected abstract void processStatistics(Statistics statistics);

    public abstract void broadcastStatistics();

}
