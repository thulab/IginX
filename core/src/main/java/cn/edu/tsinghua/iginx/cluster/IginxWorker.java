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

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.thrift.AddColumnsReq;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.CreateDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DropDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.IService;
import cn.edu.tsinghua.iginx.thrift.InsertRecordsReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.Status;

public class IginxWorker implements IService.Iface {

	private IMetaManager metaManager;

	@Override
	public OpenSessionResp openSession(OpenSessionReq req) {
		return null;
	}

	@Override
	public Status closeSession(CloseSessionReq req) {
		return null;
	}

	@Override
	public Status createDatabase(CreateDatabaseReq req) {
		return null;
	}

	@Override
	public Status dropDatabase(DropDatabaseReq req) {
		return null;
	}

	@Override
	public Status addColumns(AddColumnsReq req) {
		return null;
	}

	@Override
	public Status deleteColumns(DeleteColumnsReq req) {
		return null;
	}

	@Override
	public Status insertRecords(InsertRecordsReq req) {
		return null;
	}

	@Override
	public Status deleteDataInColumns(DeleteDataInColumnsReq req) {
		return null;
	}

	@Override
	public QueryDataResp queryData(QueryDataReq req) {
		return null;
	}
}
