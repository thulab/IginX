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
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.InsertRowRecordsContext;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DropDatabasePlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.query.async.queue.AsyncTaskQueue;
import cn.edu.tsinghua.iginx.query.async.queue.MemoryAsyncTaskQueue;
import cn.edu.tsinghua.iginx.query.async.task.AsyncTask;
import cn.edu.tsinghua.iginx.query.result.AsyncPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SyncPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.CheckedFunction.wrap;

public abstract class AbstractPlanExecutor implements IPlanExecutor, IService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractPlanExecutor.class);

    private final AsyncTaskQueue asyncTaskQueue;

    private final ExecutorService asyncTaskDispatcher;

    private final ExecutorService asyncTaskExecuteThreadPool;

    private final ExecutorService syncExecuteThreadPool;

    protected AbstractPlanExecutor() {
        asyncTaskQueue = new MemoryAsyncTaskQueue();
        asyncTaskExecuteThreadPool = Executors.newFixedThreadPool(ConfigDescriptor.getInstance().getConfig().getAsyncExecuteThreadPool());
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
                        case ADD_COLUMNS:
                            planExecuteResult = syncExecuteAddColumnsPlan((AddColumnsPlan) plan);
                            break;
                        case DELETE_COLUMNS:
                            planExecuteResult = syncExecuteDeleteColumnsPlan((DeleteColumnsPlan) plan);
                            break;
                        case DELETE_DATA_IN_COLUMNS:
                            planExecuteResult = syncExecuteDeleteDataInColumnsPlan((DeleteDataInColumnsPlan) plan);
                            break;
                        case CREATE_DATABASE:
                            planExecuteResult = syncExecuteCreateDatabasePlan((CreateDatabasePlan) plan);
                            break;
                        case DROP_DATABASE:
                            planExecuteResult = syncExecuteDropDatabasePlan((DropDatabasePlan) plan);
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
        syncExecuteThreadPool = Executors.newFixedThreadPool(ConfigDescriptor.getInstance().getConfig().getSyncExecuteThreadPool());
    }


    protected Future<NonDataPlanExecuteResult> executeInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteInsertColumnRecordsPlan(plan));
        }
        return null;
    }

    protected Future<NonDataPlanExecuteResult> executeInsertRowRecordsPlan(InsertRowRecordsPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteInsertRowRecordsPlan(plan));
        }
        return null;
    }

    protected Future<QueryDataPlanExecuteResult> executeQueryDataPlan(QueryDataPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteQueryDataPlan(plan));
        }
        return null;
    }

    protected Future<NonDataPlanExecuteResult> executeCreateDatabasePlan(CreateDatabasePlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteCreateDatabasePlan(plan));
        }
        return null;
    }

    protected Future<NonDataPlanExecuteResult> executeAddColumnPlan(AddColumnsPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteAddColumnsPlan(plan));
        }
        return null;
    }

    protected Future<NonDataPlanExecuteResult> executeDeleteColumnsPlan(DeleteColumnsPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDeleteColumnsPlan(plan));
        }
        return null;
    }

    protected Future<NonDataPlanExecuteResult> executeDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDeleteDataInColumnsPlan(plan));
        }
        return null;
    }

    protected Future<NonDataPlanExecuteResult> executeDropDatabasePlan(DropDatabasePlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteDropDatabasePlan(plan));
        }
        return null;
    }

    protected Future<AvgAggregateQueryPlanExecuteResult> executeAvgQueryPlan(AvgQueryPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteAvgQueryPlan(plan));
        }
        return null;
    }

    protected Future<StatisticsAggregateQueryPlanExecuteResult> executeCountQueryPlan(CountQueryPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteCountQueryPlan(plan));
        }
        return null;
    }

    protected Future<StatisticsAggregateQueryPlanExecuteResult> executeSumQueryPlan(SumQueryPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteSumQueryPlan(plan));
        }
        return null;
    }

    protected Future<SingleValueAggregateQueryPlanExecuteResult> executeFirstQueryPlan(FirstQueryPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteFirstQueryPlan(plan));
        }
        return null;
    }

    protected Future<SingleValueAggregateQueryPlanExecuteResult> executeLastQueryPlan(LastQueryPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteLastQueryPlan(plan));
        }
        return null;
    }

    protected Future<SingleValueAggregateQueryPlanExecuteResult> executeMaxQueryPlan(MaxQueryPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteMaxQueryPlan(plan));
        }
        return null;
    }

    protected Future<SingleValueAggregateQueryPlanExecuteResult> executeMinQueryPlan(MinQueryPlan plan) {
        if (plan.isSync()) {
            return syncExecuteThreadPool.submit(() -> syncExecuteMinQueryPlan(plan));
        }
        return null;
    }

    protected abstract NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan);

    protected abstract NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan);

    protected abstract QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan);

    protected abstract NonDataPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan);

    protected abstract NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan);

    protected abstract NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan);

    protected abstract NonDataPlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan);

    protected abstract NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan);

    protected abstract AvgAggregateQueryPlanExecuteResult syncExecuteAvgQueryPlan(AvgQueryPlan plan);

    protected abstract StatisticsAggregateQueryPlanExecuteResult syncExecuteCountQueryPlan(CountQueryPlan plan);

    protected abstract StatisticsAggregateQueryPlanExecuteResult syncExecuteSumQueryPlan(SumQueryPlan plan);

    protected abstract SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstQueryPlan(FirstQueryPlan plan);

    protected abstract SingleValueAggregateQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan);

    protected abstract SingleValueAggregateQueryPlanExecuteResult syncExecuteMaxQueryPlan(MaxQueryPlan plan);

    protected abstract SingleValueAggregateQueryPlanExecuteResult syncExecuteMinQueryPlan(MinQueryPlan plan);

    protected AsyncPlanExecuteResult executeAsyncTask(IginxPlan iginxPlan) {
        return AsyncPlanExecuteResult.getInstance(asyncTaskQueue.addAsyncTask(new AsyncTask(iginxPlan, 0)));
    }

    @Override
    public List<PlanExecuteResult> executeIginxPlans(RequestContext requestContext) {
        List<PlanExecuteResult> planExecuteResults = requestContext.getIginxPlans().stream().filter(e -> !e.isSync()).map(this::executeAsyncTask).collect(Collectors.toList());
        logger.info("" + requestContext.getType() + " has " + requestContext.getIginxPlans().size() + " sub plans");
        logger.info("there are  " + requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).count() + " sync sub plans");
        switch (requestContext.getType()) {
            case InsertColumnRecords:
                planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(InsertColumnRecordsPlan.class::cast).map(this::executeInsertColumnRecordsPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                break;
            case InsertRowRecords:
                planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(InsertRowRecordsPlan.class::cast).map(this::executeInsertRowRecordsPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                break;
            case QueryData:
                planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(QueryDataPlan.class::cast).map(this::executeQueryDataPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                break;
            case CreateDatabase:
                planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(CreateDatabasePlan.class::cast).map(this::executeCreateDatabasePlan).map(wrap(Future::get)).collect(Collectors.toList()));
                break;
            case DeleteColumns:
                planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(DeleteColumnsPlan.class::cast).map(this::executeDeleteColumnsPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                break;
            case DeleteDataInColumns:
                planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(DeleteDataInColumnsPlan.class::cast).map(this::executeDeleteDataInColumnsPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                break;
            case AddColumns:
                planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(AddColumnsPlan.class::cast).map(this::executeAddColumnPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                break;
            case DropDatabase:
                planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(DropDatabasePlan.class::cast).map(this::executeDropDatabasePlan).map(wrap(Future::get)).collect(Collectors.toList()));
                break;
            case AggregateQuery:
                AggregateType aggregateType = ((AggregateQueryContext) requestContext).getReq().aggregateType;
                switch (aggregateType) {
                    case AVG:
                        planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(AvgQueryPlan.class::cast).map(this::executeAvgQueryPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                        break;
                    case SUM:
                        planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(SumQueryPlan.class::cast).map(this::executeSumQueryPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                        break;
                    case COUNT:
                        planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(CountQueryPlan.class::cast).map(this::executeCountQueryPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                        break;
                    case MAX:
                        planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(MaxQueryPlan.class::cast).map(this::executeMaxQueryPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                        break;
                    case MIN:
                        planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(MinQueryPlan.class::cast).map(this::executeMinQueryPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                        break;
                    case FIRST:
                        planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(FirstQueryPlan.class::cast).map(this::executeFirstQueryPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                        break;
                    case LAST:
                        planExecuteResults.addAll(requestContext.getIginxPlans().stream().filter(IginxPlan::isSync).map(LastQueryPlan.class::cast).map(this::executeLastQueryPlan).map(wrap(Future::get)).collect(Collectors.toList()));
                        break;
                    default:
                        logger.error("unknown aggregate type: " + aggregateType);
                }
                break;
            default:
                logger.info("unimplemented method: " + requestContext.getType());
                break;
        }
        return planExecuteResults;
    }

    @Override
    public void shutdown() {
        asyncTaskDispatcher.shutdown();
        asyncTaskExecuteThreadPool.shutdown();
    }
}
