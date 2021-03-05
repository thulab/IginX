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

import cn.edu.tsinghua.iginx.combine.QueryDataCombineResult;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.*;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
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

	@Override
	public OpenSessionResp openSession(OpenSessionReq req) {
		logger.info("received open session request");
		if (!req.username.equals(ConfigDescriptor.getInstance().getConfig().getUsername()) ||
				!req.password.equals(ConfigDescriptor.getInstance().getConfig().getPassword())) {
			return new OpenSessionResp(RpcUtils.WRONG_PASSWORD);
		}
		long id = SnowFlakeUtils.getInstance().nextId();
		sessions.add(id);
		OpenSessionResp resp = new OpenSessionResp(RpcUtils.SUCCESS);
		resp.setSessionId(id);
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
	public Status insertRecords(InsertRecordsReq req) {
		InsertRecordsContext context = new InsertRecordsContext(req);
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

	public static IginxWorker getInstance() {
		return instance;
	}

}
