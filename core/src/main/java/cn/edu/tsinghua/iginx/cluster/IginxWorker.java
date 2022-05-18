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

import cn.edu.tsinghua.iginx.auth.SessionManager;
import cn.edu.tsinghua.iginx.auth.UserManager;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.ContextBuilder;
import cn.edu.tsinghua.iginx.engine.StatementExecutor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.storage.StorageManager;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.query.QueryManager;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.transform.driver.PythonDriver;
import cn.edu.tsinghua.iginx.transform.exec.TransformJobManager;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class IginxWorker implements IService.Iface {

    private static final Logger logger = LoggerFactory.getLogger(IginxWorker.class);

    private static final IginxWorker instance = new IginxWorker();

    private final IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final UserManager userManager = UserManager.getInstance();

    private final SessionManager sessionManager = SessionManager.getInstance();

    private final QueryManager queryManager = QueryManager.getInstance();

    private final ContextBuilder contextBuilder = ContextBuilder.getInstance();

    private final StatementExecutor executor = StatementExecutor.getInstance();

    private final PythonDriver driver = PythonDriver.getInstance();

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    public static IginxWorker getInstance() {
        return instance;
    }

    @Override
    public OpenSessionResp openSession(OpenSessionReq req) {
        String username = req.getUsername();
        String password = req.getPassword();
        if (!userManager.checkUser(username, password)) {
            OpenSessionResp resp = new OpenSessionResp(RpcUtils.WRONG_USERNAME_OR_PASSWORD);
            resp.setSessionId(0L);
            return resp;
        }
        long sessionId = sessionManager.openSession(username);
        OpenSessionResp resp = new OpenSessionResp(RpcUtils.SUCCESS);
        resp.setSessionId(sessionId);
        return resp;
    }

    @Override
    public Status closeSession(CloseSessionReq req) {
        sessionManager.closeSession(req.getSessionId());
        return RpcUtils.SUCCESS;
    }

    @Override
    public Status deleteColumns(DeleteColumnsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getStatus();
    }

    @Override
    public Status insertColumnRecords(InsertColumnRecordsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getStatus();
    }

    @Override
    public Status insertNonAlignedColumnRecords(InsertNonAlignedColumnRecordsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getStatus();
    }

    @Override
    public Status insertRowRecords(InsertRowRecordsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getStatus();
    }

    @Override
    public Status insertNonAlignedRowRecords(InsertNonAlignedRowRecordsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getStatus();
    }

    @Override
    public Status deleteDataInColumns(DeleteDataInColumnsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getStatus();
    }

    @Override
    public QueryDataResp queryData(QueryDataReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new QueryDataResp(RpcUtils.ACCESS_DENY);
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getQueryDataResp();
    }

    @Override
    public Status addStorageEngines(AddStorageEnginesReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Cluster)) {
            return RpcUtils.ACCESS_DENY;
        }
        List<StorageEngine> storageEngines = req.getStorageEngines();
        List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();

        for (StorageEngine storageEngine : storageEngines) {
            String type = storageEngine.getType();
            Map<String, String> extraParams = storageEngine.getExtraParams();
            boolean hasData = Boolean.parseBoolean(extraParams.getOrDefault(Constants.HAS_DATA, "false"));
            String dataPrefix = null;
            if (hasData && extraParams.containsKey(Constants.DATA_PREFIX)) {
                dataPrefix = extraParams.get(Constants.DATA_PREFIX);
            }
            boolean readOnly = Boolean.parseBoolean(extraParams.getOrDefault(Constants.IS_READ_ONLY, "false"));
            StorageEngineMeta meta = new StorageEngineMeta(-1, storageEngine.getIp(), storageEngine.getPort(), hasData, dataPrefix, readOnly,
                storageEngine.getExtraParams(), type, metaManager.getIginxId());
            storageEngineMetas.add(meta);

        }
        Status status = RpcUtils.SUCCESS;
        // 检测是否与已有的存储单元冲突
        List<StorageEngineMeta> currentStorageEngines = metaManager.getStorageEngineList();
        List<StorageEngineMeta> duplicatedStorageEngine = new ArrayList<>();
        for (StorageEngineMeta storageEngine : storageEngineMetas) {
            for (StorageEngineMeta currentStorageEngine : currentStorageEngines) {
                if (currentStorageEngine.getIp().equals(storageEngine.getIp()) && currentStorageEngine.getPort() == storageEngine.getPort()) {
                    duplicatedStorageEngine.add(storageEngine);
                    break;
                }
            }
        }
        if (!duplicatedStorageEngine.isEmpty()) {
            storageEngineMetas.removeAll(duplicatedStorageEngine);
            if (!storageEngines.isEmpty()) {
                status = new Status(RpcUtils.PARTIAL_SUCCESS.code);
            } else {
                status = new Status(RpcUtils.FAILURE.code);
            }
            status.setMessage("unexpected repeated add");
        }
        if (!storageEngineMetas.isEmpty()) {
            storageEngineMetas.get(storageEngineMetas.size() - 1).setLastOfBatch(true); // 每一批最后一个是 true，表示需要进行扩容
        }
        for (StorageEngineMeta meta: storageEngineMetas) {
            if (meta.isHasData()) {
                String dataPrefix = meta.getDataPrefix();
                StorageUnitMeta dummyStorageUnit = new StorageUnitMeta(Constants.DUMMY + String.format("%04d", 0), -1);
                Pair<TimeSeriesInterval, TimeInterval> boundary = StorageManager.getBoundaryOfStorage(meta);
                FragmentMeta dummyFragment;
                if (dataPrefix == null) {
                    dummyFragment = new FragmentMeta(boundary.k, boundary.v, dummyStorageUnit);
                } else {
                    dummyFragment = new FragmentMeta(new TimeSeriesInterval(dataPrefix, StringUtils.nextString(dataPrefix)), boundary.v, dummyStorageUnit);
                }
                meta.setDummyStorageUnit(dummyStorageUnit);
                meta.setDummyFragment(dummyFragment);
            }
        }
        if (!metaManager.addStorageEngines(storageEngineMetas)) {
            status = RpcUtils.FAILURE;
        }
        for (StorageEngineMeta meta : storageEngineMetas) {
            PhysicalEngineImpl.getInstance().getStorageManager().addStorage(meta);
        }
        return status;
    }

    @Override
    public AggregateQueryResp aggregateQuery(AggregateQueryReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new AggregateQueryResp(RpcUtils.ACCESS_DENY);
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getAggregateQueryResp();
    }

    @Override
    public DownsampleQueryResp downsampleQuery(DownsampleQueryReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new DownsampleQueryResp(RpcUtils.ACCESS_DENY);
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getDownSampleQueryResp();
    }

    @Override
    public ShowColumnsResp showColumns(ShowColumnsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new ShowColumnsResp(RpcUtils.ACCESS_DENY);
        }
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getShowColumnsResp();
    }

    @Override
    public GetReplicaNumResp getReplicaNum(GetReplicaNumReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new GetReplicaNumResp(RpcUtils.ACCESS_DENY);
        }
        GetReplicaNumResp resp = new GetReplicaNumResp(RpcUtils.SUCCESS);
        resp.setReplicaNum(ConfigDescriptor.getInstance().getConfig().getReplicaNum() + 1);
        return resp;
    }

    @Override
    public ExecuteSqlResp executeSql(ExecuteSqlReq req) {
        StatementExecutor executor = StatementExecutor.getInstance();
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getExecuteSqlResp();
    }

    @Override
    public LastQueryResp lastQuery(LastQueryReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new LastQueryResp(RpcUtils.ACCESS_DENY);
        }

        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        return ctx.getResult().getLastQueryResp();
    }

    @Override
    public Status updateUser(UpdateUserReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Admin)) {
            return RpcUtils.ACCESS_DENY;
        }
        if (userManager.updateUser(req.username, req.password, req.auths)) {
            return RpcUtils.SUCCESS;
        }
        return RpcUtils.FAILURE;
    }

    @Override
    public Status addUser(AddUserReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Admin)) {
            return RpcUtils.ACCESS_DENY;
        }
        if (userManager.addUser(req.username, req.password, req.auths)) {
            return RpcUtils.SUCCESS;
        }
        return RpcUtils.FAILURE;
    }

    @Override
    public Status deleteUser(DeleteUserReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Admin)) {
            return RpcUtils.ACCESS_DENY;
        }
        if (userManager.deleteUser(req.username)) {
            return RpcUtils.SUCCESS;
        }
        return RpcUtils.FAILURE;
    }

    @Override
    public GetUserResp getUser(GetUserReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new GetUserResp(RpcUtils.ACCESS_DENY);
        }
        GetUserResp resp = new GetUserResp(RpcUtils.SUCCESS);
        List<UserMeta> users;
        if (req.usernames == null) {
            users = userManager.getUsers();
        } else {
            users = userManager.getUsers(req.getUsernames());
        }
        List<String> usernames = users.stream().map(UserMeta::getUsername).collect(Collectors.toList());
        List<UserType> userTypes = users.stream().map(UserMeta::getUserType).collect(Collectors.toList());
        List<Set<AuthType>> auths = users.stream().map(UserMeta::getAuths).collect(Collectors.toList());
        resp.setUsernames(usernames);
        resp.setUserTypes(userTypes);
        resp.setAuths(auths);
        return resp;
    }

    @Override
    public GetClusterInfoResp getClusterInfo(GetClusterInfoReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Cluster)) {
            return new GetClusterInfoResp(RpcUtils.ACCESS_DENY);
        }

        GetClusterInfoResp resp = new GetClusterInfoResp();

        // IginX 信息
        List<IginxInfo> iginxInfos = new ArrayList<>();
        for (IginxMeta iginxMeta : metaManager.getIginxList()) {
            iginxInfos.add(new IginxInfo(iginxMeta.getId(), iginxMeta.getIp(), iginxMeta.getPort()));
        }
        iginxInfos.sort(Comparator.comparingLong(IginxInfo::getId));
        resp.setIginxInfos(iginxInfos);

        // 数据库信息
        List<StorageEngineInfo> storageEngineInfos = new ArrayList<>();
        for (StorageEngineMeta storageEngineMeta : metaManager.getStorageEngineList()) {
            storageEngineInfos.add(new StorageEngineInfo(storageEngineMeta.getId(), storageEngineMeta.getIp(),
                storageEngineMeta.getPort(), storageEngineMeta.getStorageEngine()));
        }
        storageEngineInfos.sort(Comparator.comparingLong(StorageEngineInfo::getId));
        resp.setStorageEngineInfos(storageEngineInfos);

        Config config = ConfigDescriptor.getInstance().getConfig();
        List<MetaStorageInfo> metaStorageInfos = null;
        LocalMetaStorageInfo localMetaStorageInfo = null;

        switch (config.getMetaStorage()) {
            case Constants.ETCD_META:
                metaStorageInfos = new ArrayList<>();
                String[] endPoints = config.getEtcdEndpoints().split(",");
                for (String endPoint : endPoints) {
                    if (endPoint.startsWith("http://")) {
                        endPoint = endPoint.substring(7);
                    } else if (endPoint.startsWith("https://")) {
                        endPoint = endPoint.substring(8);
                    }
                    String[] ipAndPort = endPoint.split(":", 2);
                    MetaStorageInfo metaStorageInfo = new MetaStorageInfo(ipAndPort[0], Integer.parseInt(ipAndPort[1]),
                        Constants.ETCD_META);
                    metaStorageInfos.add(metaStorageInfo);
                }
                break;
            case Constants.ZOOKEEPER_META:
                metaStorageInfos = new ArrayList<>();
                String[] zookeepers = config.getZookeeperConnectionString().split(",");
                for (String zookeeper : zookeepers) {
                    String[] ipAndPort = zookeeper.split(":", 2);
                    MetaStorageInfo metaStorageInfo = new MetaStorageInfo(ipAndPort[0], Integer.parseInt(ipAndPort[1]),
                        Constants.ZOOKEEPER_META);
                    metaStorageInfos.add(metaStorageInfo);
                }
                break;
            case Constants.FILE_META:
            case "":
            default:
                localMetaStorageInfo = new LocalMetaStorageInfo(
                    Paths.get(config.getFileDataDir()).toAbsolutePath().toString()
                );
        }

        if (metaStorageInfos != null) {
            resp.setMetaStorageInfos(metaStorageInfos);
        }
        if (localMetaStorageInfo != null) {
            resp.setLocalMetaStorageInfo(localMetaStorageInfo);
        }
        resp.setStatus(RpcUtils.SUCCESS);
        return resp;
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) {
        StatementExecutor executor = StatementExecutor.getInstance();
        RequestContext ctx = contextBuilder.build(req);
        executor.execute(ctx);
        queryManager.registerQuery(ctx.getId(), ctx);
        return ctx.getResult().getExecuteStatementResp(req.getFetchSize());
    }

    @Override
    public FetchResultsResp fetchResults(FetchResultsReq req) {
        RequestContext context = queryManager.getQuery(req.queryId);
        if (context == null) {
            return new FetchResultsResp(RpcUtils.SUCCESS, false);
        }
        return context.getResult().fetch(req.getFetchSize());
    }

    @Override
    public Status closeStatement(CloseStatementReq req) {
        queryManager.releaseQuery(req.queryId);
        return RpcUtils.SUCCESS;
    }

    @Override
    public CommitTransformJobResp commitTransformJob(CommitTransformJobReq req) {
        TransformJobManager manager = TransformJobManager.getInstance();
        long jobId = manager.commit(req);

        CommitTransformJobResp resp = new CommitTransformJobResp();
        if (jobId < 0) {
            resp.setStatus(RpcUtils.FAILURE);
        } else {
            resp.setStatus(RpcUtils.SUCCESS);
            resp.setJobId(jobId);
        }
        return resp;
    }

    @Override
    public QueryTransformJobStatusResp queryTransformJobStatus(QueryTransformJobStatusReq req) {
        TransformJobManager manager = TransformJobManager.getInstance();
        JobState jobState = manager.queryJobState(req.getJobId());
        if (jobState != null) {
            return new QueryTransformJobStatusResp(RpcUtils.SUCCESS, jobState);
        } else {
            return new QueryTransformJobStatusResp(RpcUtils.FAILURE, JobState.JOB_UNKNOWN);
        }
    }

    @Override
    public Status cancelTransformJob(CancelTransformJobReq req) {
        TransformJobManager manager = TransformJobManager.getInstance();
        manager.cancel(req.getSessionId());
        return RpcUtils.SUCCESS;
    }

    @Override
    public Status registerTask(RegisterTaskReq req) {
        String filePath = req.getFilePath();
        String className = req.getClassName();

        TransformTaskMeta transformTaskMeta = metaManager.getTransformTask(className);
        if (transformTaskMeta != null) {
            logger.error(String.format("Register task %s already exist", transformTaskMeta.toString()));
            return RpcUtils.FAILURE;
        }

        if (isIllegalPath(filePath)) {
            logger.error(String.format("Register file path is illegal, path=%s", filePath));
            return RpcUtils.FAILURE;
        }

        File sourceFile = new File(filePath);
        if (!sourceFile.exists()) {
            logger.error(String.format("Register file not exist in declared path, path=%s", filePath));
            return RpcUtils.FAILURE;
        }

        String fileName = sourceFile.getName();
        String destPath = System.getProperty("user.dir") + File.separator +
            "python_scripts" + File.separator + fileName;
        File destFile = new File(destPath);

        if (destFile.exists()) {
            logger.error(String.format("Register file already exist, fileName=%s", fileName));
            return RpcUtils.FAILURE;
        }

        try {
            Files.copy(sourceFile.toPath(), destFile.toPath());
        } catch (IOException e) {
            logger.error(String.format("Fail to copy register file", filePath), e);
            return RpcUtils.FAILURE;
        }

        // drive test
        if (driver.testWorker(fileName, className)) {
            metaManager.addTransformTask(new TransformTaskMeta(className, fileName, config.getIp()));
            return RpcUtils.SUCCESS;
        } else {
            return RpcUtils.FAILURE;
        }
    }

    private boolean isIllegalPath(String path) {
        //todo
        return false;
    }

    @Override
    public Status dropTask(DropTaskReq req) {
        String className = req.getClassName();
        TransformTaskMeta transformTaskMeta = metaManager.getTransformTask(className);
        if (transformTaskMeta == null) {
            logger.info("Register task not exist");
            return RpcUtils.FAILURE;
        }

        if (!transformTaskMeta.getIp().equals(config.getIp())) {
            logger.info(String.format("Register task exists in node: %s", transformTaskMeta.getIp()));
            return RpcUtils.FAILURE;
        }


        String filePath = System.getProperty("user.dir") + File.separator +
            "python_scripts" + File.separator + transformTaskMeta.getFileName();
        File file = new File(filePath);

        if (!file.exists()) {
            logger.info(String.format("Register file not exist, path=%s", filePath));
            return RpcUtils.FAILURE;
        }

        if (file.delete()) {
            metaManager.dropTransformTask(className);
            logger.info(String.format("Register file has been dropped, path=%s", filePath));
            return RpcUtils.SUCCESS;
        } else {
            logger.error(String.format("Fail to delete register file, path=%s", filePath));
            return RpcUtils.FAILURE;
        }
    }

    @Override
    public GetRegisterTaskInfoResp getRegisterTaskInfo(GetRegisterTaskInfoReq req) {
        List<TransformTaskMeta> taskMetaList = metaManager.getTransformTasks();
        List<RegisterTaskInfo> taskInfoList = new ArrayList<>();
        for (TransformTaskMeta taskMeta: taskMetaList) {
            RegisterTaskInfo taskInfo = new RegisterTaskInfo(taskMeta.getClassName(), taskMeta.getFileName(), taskMeta.getIp());
            taskInfoList.add(taskInfo);
        }
        GetRegisterTaskInfoResp resp = new GetRegisterTaskInfoResp(RpcUtils.SUCCESS);
        resp.setRegisterTaskInfoList(taskInfoList);
        return resp;
    }
}
