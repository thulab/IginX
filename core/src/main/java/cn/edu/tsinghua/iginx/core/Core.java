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

import cn.edu.tsinghua.iginx.combine.CombineExecutor;
import cn.edu.tsinghua.iginx.combine.CombineResult;
import cn.edu.tsinghua.iginx.combine.ICombineExecutor;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.core.processor.PostQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryResultCombineProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryResultCombineProcessor;
import cn.edu.tsinghua.iginx.metadatav2.IMetaManager;
import cn.edu.tsinghua.iginx.metadatav2.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.metadatav2.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.query.IPlanExecutor;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.split.IPlanGenerator;
import cn.edu.tsinghua.iginx.split.SimplePlanGenerator;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class Core {

    private static final Logger logger = LoggerFactory.getLogger(Core.class);

    private static final Core instance = new Core();

    private IPlanGenerator planGenerator;

    private IPlanExecutor queryExecutor;

    private ICombineExecutor combineExecutor;

    private final List<PreQueryPlanProcessor> preQueryPlanProcessors = new ArrayList<>();

    private final List<PostQueryPlanProcessor> postQueryPlanProcessors = new ArrayList<>();

    private final List<PreQueryExecuteProcessor> preQueryExecuteProcessors = new ArrayList<>();

    private final List<PostQueryExecuteProcessor> postQueryExecuteProcessors = new ArrayList<>();

    private final List<PreQueryResultCombineProcessor> preQueryResultCombineProcessors = new ArrayList<>();

    private final List<PostQueryResultCombineProcessor> postQueryResultCombineProcessors = new ArrayList<>();

    private final List<PostQueryProcessor> postQueryProcessors = new ArrayList<>();

    private final ExecutorService postQueryProcessThreadPool;

    private Core() {
        IMetaManager metaManager = SortedListAbstractMetaManager.getInstance();
        registerPlanGenerator(new SimplePlanGenerator());
        registerCombineExecutor(new CombineExecutor());
        try {
            Class<?> planExecutorClass = Core.class.getClassLoader().
                    loadClass(ConfigDescriptor.getInstance().getConfig().getDatabaseClassName());
            IPlanExecutor planExecutor =
                    ((Class<? extends IPlanExecutor>) planExecutorClass).getConstructor(List.class).newInstance(metaManager.getStorageEngineList());
            registerQueryExecutor(planExecutor);
            StorageEngineChangeHook hook = planExecutor.getStorageEngineChangeHook();
            if (hook != null) {
                metaManager.registerStorageEngineChangeHook(hook);
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            logger.error(e.getMessage());
        }
        IPolicy policy = PolicyManager.getInstance().getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

        registerPreQueryPlanProcessor(policy.getPreQueryPlanProcessor());
        registerPreQueryExecuteProcessor(policy.getPreQueryExecuteProcessor());
        registerPreQueryResultCombineProcessor(policy.getPreQueryResultCombineProcessor());
        registerPostQueryProcessor(policy.getPostQueryProcessor());
        registerPostQueryPlanProcessor(policy.getPostQueryPlanProcessor());
        registerPostQueryExecuteProcessor(policy.getPostQueryExecuteProcessor());
        registerPostQueryResultCombineProcessor(policy.getPostQueryResultCombineProcessor());

        postQueryProcessThreadPool = Executors.newCachedThreadPool();
    }

    public void registerPreQueryPlanProcessor(PreQueryPlanProcessor preQueryPlanProcessor) {
        if (preQueryPlanProcessor != null)
            preQueryPlanProcessors.add(preQueryPlanProcessor);
    }

    public void registerPostQueryPlanProcessor(PostQueryPlanProcessor postQueryPlanProcessor) {
        if (postQueryPlanProcessor != null)
            postQueryPlanProcessors.add(postQueryPlanProcessor);
    }

    public void registerPreQueryExecuteProcessor(PreQueryExecuteProcessor preQueryExecuteProcessor) {
        if (preQueryExecuteProcessor != null)
            preQueryExecuteProcessors.add(preQueryExecuteProcessor);
    }

    public void registerPostQueryExecuteProcessor(PostQueryExecuteProcessor postQueryExecuteProcessor) {
        if (postQueryExecuteProcessor != null)
            postQueryExecuteProcessors.add(postQueryExecuteProcessor);
    }

    public void registerPreQueryResultCombineProcessor(PreQueryResultCombineProcessor preQueryResultCombineProcessor) {
        if (preQueryResultCombineProcessor != null)
            preQueryResultCombineProcessors.add(preQueryResultCombineProcessor);
    }

    public void registerPostQueryResultCombineProcessor(PostQueryResultCombineProcessor postQueryResultCombineProcessor) {
        if (postQueryResultCombineProcessor != null)
            postQueryResultCombineProcessors.add(postQueryResultCombineProcessor);
    }

    public void registerPostQueryProcessor(PostQueryProcessor postQueryProcessor) {
        if (postQueryProcessor != null)
            postQueryProcessors.add(postQueryProcessor);
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
            Status status = processor.process(requestContext);
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
            Status status = processor.process(requestContext);
            if (status != null) {
                requestContext.setStatus(status);
                return;
            }
        }

        // 请求执行前处理器
        for (PreQueryExecuteProcessor processor: preQueryExecuteProcessors) {
            Status status = processor.process(requestContext);
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
            Status status = processor.process(requestContext);
            if (status != null) {
                requestContext.setStatus(status);
                return;
            }
        }

        // 结果合并前处理器
        for (PreQueryResultCombineProcessor processor: preQueryResultCombineProcessors) {
            Status status = processor.process(requestContext);
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
            Status status = processor.process(requestContext);
            if (status != null) {
                requestContext.setStatus(status);
                return;
            }
        }
        postQueryProcessThreadPool.submit((Callable<Void>) () -> {
            for (PostQueryProcessor processor: postQueryProcessors) {
                processor.process(requestContext);
            }
            return null;
        });
    }

    public static Core getInstance() {
        return instance;
    }

}
