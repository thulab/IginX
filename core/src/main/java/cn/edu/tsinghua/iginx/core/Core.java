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
package cn.edu.tsinghua.iginx.core;

import cn.edu.tsinghua.iginx.combine.CombineResult;
import cn.edu.tsinghua.iginx.combine.ICombineExecutor;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.core.processor.*;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.MetaManager;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.query.IPlanExecutor;
import cn.edu.tsinghua.iginx.query.iotdb.IoTDBPlanExecutor;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.split.IPlanGenerator;
import cn.edu.tsinghua.iginx.split.SimplePlanGenerator;
import cn.edu.tsinghua.iginx.thrift.Status;

import java.util.ArrayList;
import java.util.List;

public class Core {

    private static final Core instance = new Core();

    private final IMetaManager metaManager;

    private IPlanGenerator planGenerator;

    private IPlanExecutor queryExecutor;

    private ICombineExecutor combineExecutor;

    private final List<PreQueryPlanProcessor> preQueryPlanProcessors = new ArrayList<>();

    private final List<PostQueryPlanProcessor> postQueryPlanProcessors = new ArrayList<>();

    private final List<PreQueryExecuteProcessor> preQueryExecuteProcessors = new ArrayList<>();

    private final List<PostQueryExecuteProcessor> postQueryExecuteProcessors = new ArrayList<>();

    private final List<PreQueryResultCombineProcessor> preQueryResultCombineProcessors = new ArrayList<>();

    private final List<PostQueryResultCombineProcessor> postQueryResultCombineProcessors = new ArrayList<>();

    private Core() {
        metaManager = MetaManager.getInstance();
        registerPlanGenerator(new SimplePlanGenerator());
        registerQueryExecutor(new IoTDBPlanExecutor(metaManager.getDatabaseList()));
    }

    public void registerPreQueryPlanProcessor(PreQueryPlanProcessor preQueryPlanProcessor) {
        preQueryPlanProcessors.add(preQueryPlanProcessor);
    }

    public void registerPostQueryPlanProcessor(PostQueryPlanProcessor postQueryPlanProcessor) {
        postQueryPlanProcessors.add(postQueryPlanProcessor);
    }

    public void registerPreQueryExecuteProcessor(PreQueryExecuteProcessor preQueryExecuteProcessor) {
        preQueryExecuteProcessors.add(preQueryExecuteProcessor);
    }

    public void registerPostQueryExecuteProcessor(PostQueryExecuteProcessor postQueryExecuteProcessor) {
        postQueryExecuteProcessors.add(postQueryExecuteProcessor);
    }

    public void registerPreQueryResultCombineProcessor(PreQueryResultCombineProcessor preQueryResultCombineProcessor) {
        preQueryResultCombineProcessors.add(preQueryResultCombineProcessor);
    }

    public void registerPostQueryResultCombineProcessor(PostQueryResultCombineProcessor postQueryResultCombineProcessor) {
        postQueryResultCombineProcessors.add(postQueryResultCombineProcessor);
    }

    public void registerPlanGenerator(IPlanGenerator planGenerator) {
        this.planGenerator = planGenerator;
    }

    public void registerQueryExecutor(IPlanExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    public void registerCombineExecutor(ICombineExecutor combineExecutor) {
        this.combineExecutor = combineExecutor;
    }

    public void processRequest(RequestContext requestContext) {
        // 计划前处理器
        for (PreQueryPlanProcessor processor: preQueryPlanProcessors) {
            Status status = processor.queryPlanPreProcessor(requestContext);
            if (status != null) {
                requestContext.setStatus(status);
                return;
            }
        }
        // 生成计划
        List<? extends IginxPlan> iginxPlans = planGenerator.generateSubPlans(requestContext);
        requestContext.setIginxPlans(iginxPlans);
        // 计划后处理器
        for (PostQueryPlanProcessor processor: postQueryPlanProcessors) {
            Status status = processor.queryPlanPostProcess(requestContext);
            if (status != null) {
                requestContext.setStatus(status);
                return;
            }
        }

        // 请求执行前处理器
        for (PreQueryExecuteProcessor processor: preQueryExecuteProcessors) {
            Status status = processor.queryExecutePreProcessor(requestContext);
            if (status != null) {
                requestContext.setStatus(status);
                return;
            }
        }
        // 执行计划
        List<PlanExecuteResult> planExecuteResults = queryExecutor.executeIginxPlans(requestContext);
        requestContext.setPlanExecuteResults(planExecuteResults);
        // 请求执行后处理器
        for (PostQueryExecuteProcessor processor: postQueryExecuteProcessors) {
            Status status = processor.queryExecutePostProcess(requestContext);
            if (status != null) {
                requestContext.setStatus(status);
                return;
            }
        }

        // 结果合并前处理器
        for (PreQueryResultCombineProcessor processor: preQueryResultCombineProcessors) {
            Status status = processor.queryResultCombinePreProcessor(requestContext);
            if (status != null) {
                requestContext.setStatus(status);
                return;
            }
        }
        // 合并结果
        CombineResult combineResult = combineExecutor.combineResult(requestContext);
        requestContext.setCombineResult(combineResult);
        requestContext.setStatus(combineResult.getStatus());
        // 结果合并后处理器
        for (PostQueryResultCombineProcessor processor: postQueryResultCombineProcessors) {
            processor.queryResultCombinePostProcessor(requestContext);
        }
    }

    public static Core getInstance() {
        return instance;
    }

}
