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
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.*;
import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.SortedListAbstractMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class IginxWorker implements IService.Iface {

	private static final Logger logger = LoggerFactory.getLogger(IginxWorker.class);

	private static final IginxWorker instance = new IginxWorker();

	private final Set<Long> sessions = Collections.synchronizedSet(new HashSet<>());

	private final Core core = Core.getInstance();

	private final IMetaManager metaManager = SortedListAbstractMetaManager.getInstance();

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
	public Status createDatabase(CreateDatabaseReq req) {
		CreateDatabaseContext context = new CreateDatabaseContext(req);
		core.processRequest(context);
		return context.getStatus();
	}

	@Override
	public Status dropDatabase(DropDatabaseReq req) {
		DropDatabaseContext context = new DropDatabaseContext(req);
		core.processRequest(context);
		return context.getStatus();
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
		StorageEngineMeta meta = new StorageEngineMeta(0, req.getIp(), req.getPort(), req.getExtraParams(), StorageEngine.IoTDB);
		metaManager.addStorageEngine(meta);
		return RpcUtils.SUCCESS;
	}

	@Override
	public AggregateQueryResp aggregateQuery(AggregateQueryReq req) {
		AggregateQueryContext context = new AggregateQueryContext(req);
		core.processRequest(context);
		return ((AggregateCombineResult) context.getCombineResult()).getResp();
	}

	@Override
	public ValueFilterQueryResp valueFilterQuery(ValueFilterQueryReq req) throws TException
	{
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

	public static IginxWorker getInstance() {
		return instance;
	}

}
