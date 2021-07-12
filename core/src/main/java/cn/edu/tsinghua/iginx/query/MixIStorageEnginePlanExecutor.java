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
import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.AvgQueryPlan;
import cn.edu.tsinghua.iginx.plan.CountQueryPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DeleteDataInColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DropDatabasePlan;
import cn.edu.tsinghua.iginx.plan.FirstQueryPlan;
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
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.DownsampleQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.NonDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ShowColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ValueFilterQueryPlanExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MixIStorageEnginePlanExecutor extends AbstractPlanExecutor {

    private static final Logger logger = LoggerFactory.getLogger(MixIStorageEnginePlanExecutor.class);

    private final Set<Long> iotdbStorageEngines;
    private final Set<Long> influxdbStorageEngines;
    private IStorageEngine iotdbStorageEngine;
    private StorageEngineChangeHook iotdbChangeHook;
    private IStorageEngine influxdbStorageEngine;

    private StorageEngineChangeHook influxdbChangeHook;

    public MixIStorageEnginePlanExecutor(List<StorageEngineMeta> storageEngineMetaList) {
        List<StorageEngineMeta> iotdbStorageEngineMetaList = storageEngineMetaList.stream().filter(e -> e.getDbType() == StorageEngine.IoTDB)
                .collect(Collectors.toList());
        if (iotdbStorageEngineMetaList.size() != 0) {
            iotdbStorageEngine = loadStorageEngine(StorageEngine.IoTDB, iotdbStorageEngineMetaList);
            iotdbStorageEngines = Collections.synchronizedSet(iotdbStorageEngineMetaList.stream().map(StorageEngineMeta::getId).collect(Collectors.toSet()));
            iotdbChangeHook = iotdbStorageEngine.getStorageEngineChangeHook();
        } else {
            iotdbStorageEngines = Collections.synchronizedSet(new HashSet<>());
        }
        List<StorageEngineMeta> influxdbStorageEngineMetaList = storageEngineMetaList.stream().filter(e -> e.getDbType() == StorageEngine.InfluxDB)
                .collect(Collectors.toList());
        if (influxdbStorageEngineMetaList.size() != 0) {
            influxdbStorageEngine = loadStorageEngine(StorageEngine.InfluxDB, influxdbStorageEngineMetaList);
            influxdbStorageEngines = Collections.synchronizedSet(influxdbStorageEngineMetaList.stream().map(StorageEngineMeta::getId).collect(Collectors.toSet()));
            influxdbChangeHook = influxdbStorageEngine.getStorageEngineChangeHook();
        } else {
            influxdbStorageEngines = Collections.synchronizedSet(new HashSet<>());
        }
    }

    private IStorageEngine loadStorageEngine(StorageEngine storageEngine, List<StorageEngineMeta> storageEngineMetaList) {
        String[] parts = ConfigDescriptor.getInstance().getConfig().getDatabaseClassNames().split(",");
        for (String part : parts) {
            String[] kAndV = part.split("=");
            if (StorageEngine.fromString(kAndV[0]) != storageEngine) {
                continue;
            }
            String className = kAndV[1];
            try {
                Class<?> planExecutorClass = MixIStorageEnginePlanExecutor.class.getClassLoader().
                        loadClass(className);
                return ((Class<? extends IStorageEngine>) planExecutorClass)
                        .getConstructor(List.class).newInstance(storageEngineMetaList);
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                logger.error("load storage engine for " + kAndV[0] + " error, unable to create instance of " + className);
            }
        }
        throw new RuntimeException("cannot find class for " + storageEngine.name() + ", please check config.properties ");
    }

    private IStorageEngine findStorageEngine(long storageEngineId) {
        if (iotdbStorageEngines.contains(storageEngineId))
            return iotdbStorageEngine;
        if (influxdbStorageEngines.contains(storageEngineId))
            return influxdbStorageEngine;
        logger.error("unable to find storage engine: " + storageEngineId);
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteInsertColumnRecordsPlan(InsertColumnRecordsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteInsertColumnRecordsPlan(plan);
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
    public QueryDataPlanExecuteResult syncExecuteQueryDataPlan(QueryDataPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteQueryDataPlan(plan);
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteAddColumnsPlan(AddColumnsPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteAddColumnsPlan(plan);
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
    public NonDataPlanExecuteResult syncExecuteCreateDatabasePlan(CreateDatabasePlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteCreateDatabasePlan(plan);
        return null;
    }

    @Override
    public NonDataPlanExecuteResult syncExecuteDropDatabasePlan(DropDatabasePlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDropDatabasePlan(plan);
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
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteFirstQueryPlan(FirstQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteFirstQueryPlan(plan);
        return null;
    }

    @Override
    public SingleValueAggregateQueryPlanExecuteResult syncExecuteLastQueryPlan(LastQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteLastQueryPlan(plan);
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
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleFirstQueryPlan(DownsampleFirstQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleFirstQueryPlan(plan);
        return null;
    }

    @Override
    public DownsampleQueryPlanExecuteResult syncExecuteDownsampleLastQueryPlan(DownsampleLastQueryPlan plan) {
        IStorageEngine storageEngine = findStorageEngine(plan.getStorageEngineId());
        if (storageEngine != null)
            return storageEngine.syncExecuteDownsampleLastQueryPlan(plan);
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
                logger.info("a new storage engine added: " + after.getIp() + ":" + after.getPort() + "-" + after.getDbType());
                switch (after.getDbType()) {
                    case IoTDB:
                        iotdbStorageEngines.add(after.getId());
                        if (iotdbStorageEngine == null) {
                            iotdbStorageEngine = loadStorageEngine(StorageEngine.IoTDB, Collections.singletonList(after));
                            iotdbChangeHook = iotdbStorageEngine.getStorageEngineChangeHook();
                        } else {
                            iotdbChangeHook.onChanged(null, after);
                        }
                        break;
                    case InfluxDB:
                        influxdbStorageEngines.add(after.getId());
                        if (influxdbStorageEngine == null) {
                            influxdbStorageEngine = loadStorageEngine(StorageEngine.InfluxDB, Collections.singletonList(after));
                            influxdbChangeHook = influxdbStorageEngine.getStorageEngineChangeHook();
                        } else {
                            influxdbChangeHook.onChanged(null, after);
                        }
                        break;
                }
            }
        };
    }

}
