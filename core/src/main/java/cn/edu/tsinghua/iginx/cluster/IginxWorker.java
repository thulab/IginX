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
import cn.edu.tsinghua.iginx.combine.AggregateCombineResult;
import cn.edu.tsinghua.iginx.combine.DownsampleQueryCombineResult;
import cn.edu.tsinghua.iginx.combine.LastQueryCombineResult;
import cn.edu.tsinghua.iginx.combine.QueryDataCombineResult;
import cn.edu.tsinghua.iginx.combine.ShowColumnsCombineResult;
import cn.edu.tsinghua.iginx.combine.ValueFilterCombineResult;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.DeleteColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DeleteDataInColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.InsertColumnRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertNonAlignedColumnRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertNonAlignedRowRecordsContext;
import cn.edu.tsinghua.iginx.core.context.InsertRowRecordsContext;
import cn.edu.tsinghua.iginx.core.context.LastQueryContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.ShowColumnsContext;
import cn.edu.tsinghua.iginx.core.context.ValueFilterQueryContext;
import cn.edu.tsinghua.iginx.exceptions.SQLParserException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.query.MixIStorageEnginePlanExecutor;
import cn.edu.tsinghua.iginx.sql.IginXSqlVisitor;
import cn.edu.tsinghua.iginx.sql.SQLParseError;
import cn.edu.tsinghua.iginx.sql.SqlLexer;
import cn.edu.tsinghua.iginx.sql.SqlParser;
import cn.edu.tsinghua.iginx.sql.operator.Operator;
import cn.edu.tsinghua.iginx.thrift.AddStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.AddUserReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteUserReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoReq;
import cn.edu.tsinghua.iginx.thrift.GetClusterInfoResp;
import cn.edu.tsinghua.iginx.thrift.GetReplicaNumReq;
import cn.edu.tsinghua.iginx.thrift.GetReplicaNumResp;
import cn.edu.tsinghua.iginx.thrift.GetUserReq;
import cn.edu.tsinghua.iginx.thrift.GetUserResp;
import cn.edu.tsinghua.iginx.thrift.IService;
import cn.edu.tsinghua.iginx.thrift.IginxInfo;
import cn.edu.tsinghua.iginx.thrift.InsertColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.LastQueryReq;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;
import cn.edu.tsinghua.iginx.thrift.LocalMetaStorageInfo;
import cn.edu.tsinghua.iginx.thrift.MetaStorageInfo;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.thrift.StorageEngineInfo;
import cn.edu.tsinghua.iginx.thrift.UpdateUserReq;
import cn.edu.tsinghua.iginx.thrift.UserType;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryReq;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryResp;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class IginxWorker implements IService.Iface {

    private static final Logger logger = LoggerFactory.getLogger(IginxWorker.class);

    private static final IginxWorker instance = new IginxWorker();

    private final Core core = Core.getInstance();

    private final IMetaManager metaManager = DefaultMetaManager.getInstance();

    private final UserManager userManager = UserManager.getInstance();

    private final SessionManager sessionManager = SessionManager.getInstance();

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
        DeleteColumnsContext context = new DeleteColumnsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status insertColumnRecords(InsertColumnRecordsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        InsertColumnRecordsContext context = new InsertColumnRecordsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status insertNonAlignedColumnRecords(InsertNonAlignedColumnRecordsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        InsertNonAlignedColumnRecordsContext context = new InsertNonAlignedColumnRecordsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status insertRowRecords(InsertRowRecordsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        InsertRowRecordsContext context = new InsertRowRecordsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status insertNonAlignedRowRecords(InsertNonAlignedRowRecordsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        InsertNonAlignedRowRecordsContext context = new InsertNonAlignedRowRecordsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public Status deleteDataInColumns(DeleteDataInColumnsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Write)) {
            return RpcUtils.ACCESS_DENY;
        }
        DeleteDataInColumnsContext context = new DeleteDataInColumnsContext(req);
        core.processRequest(context);
        return context.getStatus();
    }

    @Override
    public QueryDataResp queryData(QueryDataReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new QueryDataResp(RpcUtils.ACCESS_DENY);
        }
        QueryDataContext context = new QueryDataContext(req);
        core.processRequest(context);
        return ((QueryDataCombineResult) context.getCombineResult()).getResp();
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
            StorageEngineMeta meta = new StorageEngineMeta(0, storageEngine.getIp(), storageEngine.getPort(),
                    storageEngine.getExtraParams(), type, metaManager.getIginxId());
            try {
                if (!MixIStorageEnginePlanExecutor.testConnection(meta)) {
                    return RpcUtils.FAILURE;
                }
            } catch (Exception e) {
                logger.error("load storage engine error, unable to connection to " + meta.getIp() + ":" + meta.getPort());
                return RpcUtils.FAILURE;
            }
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
        if (!metaManager.addStorageEngines(storageEngineMetas)) {
            status = RpcUtils.FAILURE;
        }
        return status;
    }

    @Override
    public AggregateQueryResp aggregateQuery(AggregateQueryReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new AggregateQueryResp(RpcUtils.ACCESS_DENY);
        }
        AggregateQueryContext context = new AggregateQueryContext(req);
        core.processRequest(context);
        return ((AggregateCombineResult) context.getCombineResult()).getResp();
    }

    @Override
    public ValueFilterQueryResp valueFilterQuery(ValueFilterQueryReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new ValueFilterQueryResp(RpcUtils.ACCESS_DENY);
        }
        ValueFilterQueryContext context = new ValueFilterQueryContext(req);
        core.processRequest(context);
        return ((ValueFilterCombineResult) context.getCombineResult()).getResp();
    }

    @Override
    public DownsampleQueryResp downsampleQuery(DownsampleQueryReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new DownsampleQueryResp(RpcUtils.ACCESS_DENY);
        }
        DownsampleQueryContext context = new DownsampleQueryContext(req);
        core.processRequest(context);
        return ((DownsampleQueryCombineResult) context.getCombineResult()).getResp();
    }

    @Override
    public ShowColumnsResp showColumns(ShowColumnsReq req) {
        if (!sessionManager.checkSession(req.getSessionId(), AuthType.Read)) {
            return new ShowColumnsResp(RpcUtils.ACCESS_DENY);
        }
        ShowColumnsContext context = new ShowColumnsContext(req);
        core.processRequest(context);
        return ((ShowColumnsCombineResult) context.getCombineResult()).getResp();
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
        SqlLexer lexer = new SqlLexer(CharStreams.fromString(req.getStatement()));
        lexer.removeErrorListeners();
        lexer.addErrorListener(SQLParseError.INSTANCE);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        SqlParser parser = new SqlParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(SQLParseError.INSTANCE);

        IginXSqlVisitor visitor = new IginXSqlVisitor();

        try {
            ParseTree tree = parser.sqlStatement();
            Operator operator = visitor.visit(tree);
            return operator.doOperation(req.getSessionId());
        } catch (SQLParserException | ParseCancellationException e) {
            StatusCode statusCode =  StatusCode.STATEMENT_PARSE_ERROR;
            String errMsg = e.getMessage();
            ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.status(statusCode, errMsg), SqlType.Unknown);
            resp.setParseErrorMsg(e.getMessage());
            return resp;
        } catch (Exception e) {
            e.printStackTrace();
            ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.FAILURE, SqlType.Unknown);
            resp.setParseErrorMsg("Execute Error: encounter error(s) when executing sql statement, " +
                    "see server log for more details.");
            return resp;
        }
    }

    @Override
    public LastQueryResp lastQuery(LastQueryReq req) {
        LastQueryContext context = new LastQueryContext(req);
        core.processRequest(context);
        return ((LastQueryCombineResult) context.getCombineResult()).getResp();
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
        for (IginxMeta iginxMeta: metaManager.getIginxList()) {
            iginxInfos.add(new IginxInfo(iginxMeta.getId(), iginxMeta.getIp(), iginxMeta.getPort()));
        }
        iginxInfos.sort(Comparator.comparingLong(IginxInfo::getId));
        resp.setIginxInfos(iginxInfos);

        // 数据库信息
        List<StorageEngineInfo> storageEngineInfos = new ArrayList<>();
        for (StorageEngineMeta storageEngineMeta: metaManager.getStorageEngineList()) {
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
                for (String endPoint: endPoints) {
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
                for (String zookeeper: zookeepers) {
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
}
