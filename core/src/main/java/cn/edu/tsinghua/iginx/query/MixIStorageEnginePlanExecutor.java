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
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.FirstValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertNonAlignedRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.plan.LastQueryPlan;
import cn.edu.tsinghua.iginx.plan.LastValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.MaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.MinQueryPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.plan.ShowColumnsPlan;
import cn.edu.tsinghua.iginx.plan.SumQueryPlan;
import cn.edu.tsinghua.iginx.plan.ValueFilterQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleAvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleCountQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleFirstValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleLastValueQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMaxQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleMinQueryPlan;
import cn.edu.tsinghua.iginx.plan.downsample.DownsampleSumQueryPlan;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DownsampleQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.LastQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ShowColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ValueFilterQueryPlanExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MixIStorageEnginePlanExecutor extends AbstractPlanExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MixIStorageEnginePlanExecutor.class);

    private final Map<String, ClassLoader> classLoaders = new ConcurrentHashMap<>();
    private final Map<String, IStorageEngine> storageEngines = new ConcurrentHashMap<>();
    private final Map<Long, IStorageEngine> storageEngineMap = new ConcurrentHashMap<>();
    private final Map<String, StorageEngineChangeHook> hooks = new ConcurrentHashMap<>();

    public MixIStorageEnginePlanExecutor(List<StorageEngineMeta> storageEngineMetaList) {
        try {
            Map<String, List<StorageEngineMeta>> groupedStorageEngineMetaLists = storageEngineMetaList.stream()
                    .collect(Collectors.groupingBy(StorageEngineMeta::getStorageEngine));
            for (String engine : groupedStorageEngineMetaLists.keySet()) {
                ClassLoader classLoader = new StorageEngineClassLoader(engine);
                classLoaders.put(engine, classLoader);

                IStorageEngine storageEngine = (IStorageEngine) classLoader.loadClass(getDriverClassName(engine))
                        .getConstructor(List.class).newInstance(groupedStorageEngineMetaLists.get(engine));

                storageEngines.put(engine, storageEngine);
                hooks.put(engine, storageEngine.getStorageEngineChangeHook());
                for (StorageEngineMeta meta : groupedStorageEngineMetaLists.get(engine)) {
                    storageEngineMap.put(meta.getId(), storageEngine);
                }
            }
        } catch (Exception e) {
            logger.error("encounter error when init MixIStorageEnginePlanExecutor: ", e);
            System.exit(1);
        }
    }

    public static boolean testConnection(StorageEngineMeta meta) throws Exception {
        ClassLoader classLoader = new StorageEngineClassLoader(meta.getStorageEngine());
        Class<?> planExecutorClass = classLoader.loadClass(getDriverClassName(meta.getStorageEngine()));
        Method method = planExecutorClass.getMethod("testConnection", StorageEngineMeta.class);
        return (boolean) method.invoke(null, meta);
    }

    private static String getDriverClassName(String storageEngine) {
        String[] parts = ConfigDescriptor.getInstance().getConfig().getDatabaseClassNames().split(",");
        for (String part : parts) {
            String[] kAndV = part.split("=");
            if (!kAndV[0].equals(storageEngine)) {
                continue;
            }
            return kAndV[1];
        }
        throw new RuntimeException("cannot find driver for " + storageEngine + ", please check config.properties ");
    }

    private IStorageEngine findStorageEngine(long storageEngineId) {
        return storageEngineMap.get(storageEngineId);
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteInsertColumnRecordsPlan(plan);
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertNonAlignedColumnRecordsPlan(InsertNonAlignedColumnRecordsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteInsertNonAlignedColumnRecordsPlan(plan);
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertRowRecordsPlan(InsertRowRecordsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteInsertRowRecordsPlan(plan);
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertNonAlignedRowRecordsPlan(InsertNonAlignedRowRecordsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteInsertNonAlignedRowRecordsPlan(plan);
        return null;
    }

    @Override
    public QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteQueryDataPlan(plan);
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteColumnsPlan(DeleteColumnsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDeleteColumnsPlan(plan);
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDeleteDataInColumnsPlan(DeleteDataInColumnsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDeleteDataInColumnsPlan(plan);
        return null;
    }

    @Override
    public LastQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteLastQueryPlan(plan);
        return null;
    }

    @Override
    public AvgAggregateQueryPlanExecuteResult syncExecuteAvgQueryPlan(AvgQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteAvgQueryPlan(plan);
        return null;
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteCountQueryPlan(CountQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteCountQueryPlan(plan);
        return null;
    }

    @Override
    public StatisticsAggregateQueryPlanExecuteResult syncExecuteSumQueryPlan(SumQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteSumQueryPlan(plan);
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstValueQueryPlan(FirstValueQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteFirstValueQueryPlan(plan);
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteLastValueQueryPlan(LastValueQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteLastValueQueryPlan(plan);
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMaxQueryPlan(MaxQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteMaxQueryPlan(plan);
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteMinQueryPlan(MinQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteMinQueryPlan(plan);
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleAvgQueryPlan(DownsampleAvgQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleAvgQueryPlan(plan);
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleCountQueryPlan(DownsampleCountQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleCountQueryPlan(plan);
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleSumQueryPlan(DownsampleSumQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleSumQueryPlan(plan);
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMaxQueryPlan(DownsampleMaxQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleMaxQueryPlan(plan);
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleMinQueryPlan(DownsampleMinQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleMinQueryPlan(plan);
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstValueQueryPlan(DownsampleFirstValueQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleFirstValueQueryPlan(plan);
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastValueQueryPlan(DownsampleLastValueQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleLastValueQueryPlan(plan);
        return null;
    }

    @Override
    public ValueFilterQueryPlanExecuteResult syncExecuteValueFilterQueryPlan(ValueFilterQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteValueFilterQueryPlan(plan);
        return null;
    }

    @Override
    public ShowColumnsPlanExecuteResult syncExecuteShowColumnsPlan(ShowColumnsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteShowColumnsPlan(plan);
        return null;
    }

    @Override
    public StorageEngineChangeHook getStorageEngineChangeHook() {
        return (before, after) -> {
            if (before == null && after != null) {
                logger.info("a new storage engine added: " + after.getIp() + ":" + after.getPort() + "-" + after.getStorageEngine());
                String engine = after.getStorageEngine();
                if (storageEngines.containsKey(engine)) { // 已有的引擎新增数据节点
                    hooks.get(engine).onChanged(null, after);
                } else {
                    try {
                        ClassLoader classLoader = new StorageEngineClassLoader(engine);
                        classLoaders.put(engine, classLoader);

                        IStorageEngine storageEngine = (IStorageEngine) classLoader.loadClass(getDriverClassName(engine))
                                .getConstructor(List.class).newInstance(Collections.singletonList(after));

                        storageEngines.put(engine, storageEngine);
                        hooks.put(engine, storageEngine.getStorageEngineChangeHook());
                        storageEngineMap.put(after.getId(), storageEngine);

                    } catch (Exception e) {
                        logger.error("init storage engine " + engine + " error when change storage engines.");
                    }
                }
            }
        };
    }

}
