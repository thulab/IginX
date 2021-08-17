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
package cn.edu.tsinghua.iginx.query;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.IService;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.ShowColumnsPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.ValueFilterQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.query.async.queue.AsyncTaskQueue;
import cn.edu.tsinghua.iginx.query.async.queue.MemoryAsyncTaskQueue;
import cn.edu.tsinghua.iginx.query.async.task.AsyncTask;
import cn.edu.tsinghua.iginx.query.result.AsyncPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SyncPlanExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.CheckedFunction.wrap;

public abstract class AbstractPlanExecutor implements IPlanExecutor, IService, IStorageEngine {

    private static final Logger logger = LoggerFactory.getLogger(AbstractPlanExecutor.class);

    private final AsyncTaskQueue asyncTaskQueue;

    private final ExecutorService asyncTaskDispatcher;

    private final ExecutorService asyncTaskExecuteThreadPool;

    private final ExecutorService syncExecuteThreadPool;

    private final Map<IginxPlan.IginxPlanType, Function<IginxPlan, Future<? extends PlanExecuteResult>>> functionMap = new HashMap<>();

    protected AbstractPlanExecutor() {
        asyncTaskQueue = new MemoryAsyncTaskQueue();
        asyncTaskExecuteThreadPool = Executors.newFixedThreadPool(ConfigDescriptor.getInstance().getConfig().getAsyncExecuteThreadPoolSize());
        asyncTaskDispatcher = Executors.newSingleThreadExecutor();
        asyncTaskDispatcher.submit(() -> {
            while (true) {
                AsyncTask asyncTask = asyncTaskQueue.getAsyncTask();
                asyncTaskExecuteThreadPool.submit(() -> {
                    IginxPlan plan = asyncTask.getIginxPlan();
                    SyncPlanExecuteResult planExecuteResult = null;
                    switch (plan.getIginxPlanType()) {
                        case INSERT_COLUMN_RECORDS:
                            logger.info("execute async insert column records task");
                            planExecuteResult = syncExecuteInsertColumnRecordsPlan((InsertColumnRecordsPlan) plan);
                            break;
                        case INSERT_ROW_RECORDS:
                            logger.info("execute async insert row records task");
                            planExecuteResult = syncExecuteInsertRowRecordsPlan((InsertRowRecordsPlan) plan);
                            break;
                        case DELETE_COLUMNS:
                            planExecuteResult = syncExecuteDeleteColumnsPlan((DeleteColumnsPlan) plan);
                            break;
                        case DELETE_DATA_IN_COLUMNS:
                            planExecuteResult = syncExecuteDeleteDataInColumnsPlan((DeleteDataInColumnsPlan) plan);
                            break;
                        default:
                            logger.info("unimplemented method: " + plan.getIginxPlanType());
                    }
                    if (planExecuteResult == null || planExecuteResult.getStatusCode() != PlanExecuteResult.SUCCESS) { // 异步任务执行失败后再次执行，直到到达预设的最大执行次数
                        asyncTask.addRetryTimes();
                        if (asyncTask.getRetryTimes() < ConfigDescriptor.getInstance().getConfig().getMaxAsyncRetryTimes()) {
                            asyncTaskQueue.addAsyncTask(asyncTask);
                        }
                    }
                });
            }
        });
        syncExecuteThreadPool = Executors.newFixedThreadPool(ConfigDescriptor.getInstance().getConfig().getSyncExecuteThreadPoolSize());

        initFunctionMap();
    }

    private void initFunctionMap() {
        functionMap.put(IginxPlan.IginxPlanType.INSERT_COLUMN_RECORDS, this::executeInsertColumnRecordsPlan);
        functionMap.put(IginxPlan.IginxPlanType.INSERT_ROW_RECORDS, this::executeInsertRowRecordsPlan);
        functionMap.put(IginxPlan.IginxPlanType.QUERY_DATA, this::executeQueryDataPlan);
        functionMap.put(IginxPlan.IginxPlanType.DELETE_COLUMNS, this::executeDeleteColumnsPlan);
        functionMap.put(IginxPlan.IginxPlanType.DELETE_DATA_IN_COLUMNS, this::executeDeleteDataInColumnsPlan);
        functionMap.put(IginxPlan.IginxPlanType.AVG, this::executeAvgQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.SUM, this::executeSumQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.COUNT, this::executeCountQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.MAX, this::executeMaxQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.MIN, this::executeMinQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.FIRST, this::executeFirstQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.LAST, this::executeLastQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.DOWNSAMPLE_AVG, this::executeDownsampleAvgQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.DOWNSAMPLE_SUM, this::executeDownsampleSumQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.DOWNSAMPLE_COUNT, this::executeDownsampleCountQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.DOWNSAMPLE_MAX, this::executeDownsampleMaxQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.DOWNSAMPLE_MIN, this::executeDownsampleMinQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.DOWNSAMPLE_FIRST, this::executeDownsampleFirstQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.DOWNSAMPLE_LAST, this::executeDownsampleLastQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.VALUE_FILTER_QUERY, this::executeValueFilterQueryPlan);
        functionMap.put(IginxPlan.IginxPlanType.SHOW_COLUMNS, this::executeShowColumnsPlan);
    }


    protected Future<? extends PlanExecuteResult> executeInsertColumnRecordsPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteInsertColumnRecordsPlan((InsertColumnRecordsPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeInsertRowRecordsPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteInsertRowRecordsPlan((InsertRowRecordsPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeQueryDataPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteQueryDataPlan((QueryDataPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeValueFilterQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteValueFilterQueryPlan((ValueFilterQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDeleteColumnsPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDeleteColumnsPlan((DeleteColumnsPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDeleteDataInColumnsPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDeleteDataInColumnsPlan((DeleteDataInColumnsPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeAvgQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteAvgQueryPlan((AvgQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeCountQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteCountQueryPlan((CountQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeSumQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteSumQueryPlan((SumQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeFirstQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteFirstQueryPlan((FirstQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeLastQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteLastQueryPlan((LastQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeMaxQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteMaxQueryPlan((MaxQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeMinQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteMinQueryPlan((MinQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDownsampleAvgQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDownsampleAvgQueryPlan((DownsampleAvgQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDownsampleCountQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDownsampleCountQueryPlan((DownsampleCountQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDownsampleSumQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDownsampleSumQueryPlan((DownsampleSumQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDownsampleMaxQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDownsampleMaxQueryPlan((DownsampleMaxQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDownsampleMinQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDownsampleMinQueryPlan((DownsampleMinQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDownsampleFirstQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDownsampleFirstQueryPlan((DownsampleFirstQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeDownsampleLastQueryPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDownsampleLastQueryPlan((DownsampleLastQueryPlan) plan));
        }
        return null;
    }

    protected Future<? extends PlanExecuteResult> executeShowColumnsPlan(IginxPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteShowColumnsPlan((ShowColumnsPlan) plan));
        }
        return null;
    }

    protected AsyncPlanExecuteResult executeAsyncTask(IginxPlan iginxPlan) {
        return AsyncPlanExecuteResult.getInstance(asyncTaskQueue.addAsyncTask(new AsyncTask(iginxPlan, 0)));
    }

    @Override
    public List<PlanExecuteResult> executeIginxPlans(RequestContext requestContext) {
        List<PlanExecuteResult> planExecuteResults = requestContext.getIginxPlans().stream().filter(e -> !e.isSync()).map(this::executeAsyncTask).collect(Collectors.toList());
        logger.debug(requestContext.getType() + " has " + requestContext.getIginxPlans().size() + " sub plans");
        logger.debug("there are  " + requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).count() + " sync sub plans");
        planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(e -> functionMap.get(e.getIginxPlanType()).apply(e)).map(wrap(Future::get)).collect(Collectors.toList()));
        return planExecuteResults;
    }

    @Override
    public void shutdown() {
        asyncTaskDispatcher.shutdown();
        asyncTaskExecuteThreadPool.shutdown();
    }
}
