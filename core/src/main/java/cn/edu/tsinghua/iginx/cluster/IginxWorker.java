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
import cn.edu.tsinghua.iginx.combine.ShowColumnsCombineResult;
import cn.edu.tsinghua.iginx.combine.ValueFilterCombineResult;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.DeleteColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DeleteDataInColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.InsertColumnRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertRowRecordsContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.ShowColumnsContext;
import cn.edu.tsinghua.iginx.core.context.ValueFilterQueryContext;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.query.MixIStorageEnginePlanExecutor;
import cn.edu.tsinghua.iginx.thrift.AddStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.GetReplicaNumReq;
import cn.edu.tsinghua.iginx.thrift.GetReplicaNumResp;
import cn.edu.tsinghua.iginx.thrift.IService;
import cn.edu.tsinghua.iginx.thrift.InsertColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryReq;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryResp;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IginxWorker implements IService.Iface {

    private static final Logger logger = LoggerFactory.getLogger(IginxWorker.class);

    private static final IginxWorker instance = new IginxWorker();

    private final Set<Long> sessions = Collections.synchronizedSet(new HashSet<>());

    private final Core core = Core.getInstance();

    private final IMetaManager metaManager = DefaultMetaManager.getInstance();

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
    public Status addStorageEngines(AddStorageEnginesReq req) {
        List<StorageEngine> storageEngines = req.getStorageEngines();
        List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();

        Map<cn.edu.tsinghua.iginx.core.db.StorageEngine, Method> checkConnectionMethods = new HashMap<>();
        String[] driverInfos = ConfigDescriptor.getInstance().getConfig().getDatabaseClassNames().split(",");
        for (String driverInfo: driverInfos) {
            String[] kAndV = driverInfo.split("=");
            String className = kAndV[1];
            try {
                Class<?> planExecutorClass = MixIStorageEnginePlanExecutor.class.getClassLoader().
                        loadClass(className);
                Method method = planExecutorClass.getMethod("testConnection", StorageEngineMeta.class);
                checkConnectionMethods.put(cn.edu.tsinghua.iginx.core.db.StorageEngine.fromString(kAndV[0]), method);
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalArgumentException e) {
                logger.error("load storage engine for " + kAndV[0] + " error, unable to create instance of " + className);
            }
        }

        for (StorageEngine storageEngine: storageEngines) {
            cn.edu.tsinghua.iginx.core.db.StorageEngine type = cn.edu.tsinghua.iginx.core.db.StorageEngine.fromThrift(storageEngine.getType());
            StorageEngineMeta meta = new StorageEngineMeta(0, storageEngine.getIp(), storageEngine.getPort(),
                    storageEngine.getExtraParams(), type, metaManager.getIginxId());
            Method checkConnectionMethod = checkConnectionMethods.get(type);
            if (checkConnectionMethod == null) {
                logger.error("unsupported storage engine " + type);
                return RpcUtils.FAILURE;
            }
            try {
                if (!((boolean) checkConnectionMethod.invoke(null, meta))) {
                    return RpcUtils.FAILURE;
                }
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                logger.error("load storage engine error, unable to connection to " + meta.getIp() + ":" + meta.getPort());
                return RpcUtils.FAILURE;
            }
            storageEngineMetas.add(meta);

        }
        if (!storageEngineMetas.isEmpty()) {
            storageEngineMetas.get(storageEngineMetas.size() - 1).setLastOfBatch(true); // 每一批最后一个是 true，表示需要进行扩容
        }
        if (!metaManager.addStorageEngines(storageEngineMetas)) {
            return RpcUtils.FAILURE;
        }
        return RpcUtils.SUCCESS;
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

    @Override
    public ShowColumnsResp showColumns(ShowColumnsReq req) {
        ShowColumnsContext context = new ShowColumnsContext(req);
        core.processRequest(context);
        return ((ShowColumnsCombineResult) context.getCombineResult()).getResp();
    }

    @Override
    public GetReplicaNumResp getReplicaNum(GetReplicaNumReq req) {
        return new GetReplicaNumResp(RpcUtils.SUCCESS, ConfigDescriptor.getInstance().getConfig().getReplicaNum() + 1);
    }
}
