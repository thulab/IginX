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
package cn.edu.tsinghua.iginx.cluster;

import cn.edu.tsinghua.iginx.combine.AggregateCombineResult;
import cn.edu.tsinghua.iginx.combine.DownsampleQueryCombineResult;
import cn.edu.tsinghua.iginx.combine.QueryDataCombineResult;
import cn.edu.tsinghua.iginx.combine.ValueFilterCombineResult;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.AddColumnsContext;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.DeleteColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DeleteDataInColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.InsertColumnRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertRowRecordsContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.ValueFilterQueryContext;
import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.query.MixIStorageEnginePlanExecutor;
import cn.edu.tsinghua.iginx.thrift.AddColumnsReq;
import cn.edu.tsinghua.iginx.thrift.AddStorageEngineReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.IService;
import cn.edu.tsinghua.iginx.thrift.InsertColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryReq;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryResp;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IginxWorker implements IService.Iface {

    private static final Logger logger = LoggerFactory.getLogger(IginxWorker.class);

    private static final IginxWorker instance = new IginxWorker();

    private final Set<Long> sessions = Collections.synchronizedSet(new HashSet<>());

    private final Core core = Core.getInstance();

    private final IMetaManager metaManager = SortedListAbstractMetaManager.getInstance();

    public static IginxWorker getInstance() {
        return instance;
    }

    @Override
    public OpenSessionResp openSession(OpenSessionReq req) {
        logger.info("received open session request");
        logger.info("start to generate test id");
        long id = SnowFlakeUtils.getInstance().nextId();
        logger.info("generate session id: " + id);
        sessions.add(id);
        logger.info("add session " + id + " into set");
        OpenSessionResp resp = new OpenSessionResp(RpcUtils.SUCCESS);
        resp.setSessionId(id);
        logger.info("return request");
        return resp;
    }

    @Override
    public Status closeSession(CloseSessionReq req) {
        if (!sessions.contains(req.getSessionId())) {
            return RpcUtils.INVALID_SESSION;
        }
        sessions.remove(req.sessionId);
        return RpcUtils.SUCCESS;
    }

    @Override
    public Status addColumns(AddColumnsReq req) {
        AddColumnsContext context = new AddColumnsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status deleteColumns(DeleteColumnsReq req) {
        DeleteColumnsContext context = new DeleteColumnsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status insertColumnRecords(InsertColumnRecordsReq req) {
        InsertColumnRecordsContext context = new InsertColumnRecordsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status insertRowRecords(InsertRowRecordsReq req) {
        InsertRowRecordsContext context = new InsertRowRecordsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status deleteDataInColumns(DeleteDataInColumnsReq req) {
        DeleteDataInColumnsContext context = new DeleteDataInColumnsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public QueryDataResp queryData(QueryDataReq req) {
        QueryDataContext context = new QueryDataContext(req);
        core.processRequest(context);
        return ((QueryDataCombineResult) context.getCombineResult()).getResp();
    }

    @Override
    public Status addStorageEngine(AddStorageEngineReq req) {
        // 处理扩容
        StorageEngine storageEngine = StorageEngine.fromThrift(req.type);
        StorageEngineMeta meta = new StorageEngineMeta(0, req.getIp(), req.getPort(), req.getExtraParams(), storageEngine);
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
                Method method = planExecutorClass.getMethod("testConnection", StorageEngineMeta.class);
                if (!((boolean) method.invoke(null, meta))) {
                    return RpcUtils.FAILURE;
                }
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                logger.error("load storage engine for " + kAndV[0] + " error, unable to create instance of " + className);
            }
        }
        metaManager.addStorageEngine(meta);
        Map<String, Long> migrationMap = metaManager.selectStorageUnitsToMigrate(Collections.singletonList(meta.getId()));
        boolean success = true;
        for (Map.Entry<String, Long> entry : migrationMap.entrySet()) {
            success &= metaManager.migrateStorageUnit(entry.getKey(), entry.getValue());
        }
        return success ? RpcUtils.SUCCESS : RpcUtils.FAILURE;
    }

    @Override
    public AggregateQueryResp aggregateQuery(AggregateQueryReq req) {
        AggregateQueryContext context = new AggregateQueryContext(req);
        core.processRequest(context);
        return ((AggregateCombineResult) context.getCombineResult()).getResp();
    }

    @Override
    public ValueFilterQueryResp valueFilterQuery(ValueFilterQueryReq req) {
        ValueFilterQueryContext context = new ValueFilterQueryContext(req);
        core.processRequest(context);
        return ((ValueFilterCombineResult) context.getCombineResult()).getResp();
    }

    @Override
    public DownsampleQueryResp downsampleQuery(DownsampleQueryReq req) {
        DownsampleQueryContext context = new DownsampleQueryContext(req);
        core.processRequest(context);
        return ((DownsampleQueryCombineResult) context.getCombineResult()).getResp();
    }

}
