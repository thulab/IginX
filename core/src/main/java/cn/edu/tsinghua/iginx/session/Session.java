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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.AddColumnsReq;
import cn.edu.tsinghua.iginx.thrift.AddStorageEngineReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.CreateDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DropDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.IService;
import cn.edu.tsinghua.iginx.thrift.InsertColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.InsertRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteBufferByDataType;

public class Session {

	private static final Logger logger = LoggerFactory.getLogger(Session.class);

	private String host;
	private int port;
	private String username;
	private String password;
	private IService.Iface client;
	private long sessionId;
	private TTransport transport;
	private boolean isClosed;

	public Session(String host, int port) {
		this(host, port, Constants.DEFAULT_USERNAME, Constants.DEFAULT_PASSWORD);
	}

	public Session(String host, String port) {
		this(host, port, Constants.DEFAULT_USERNAME, Constants.DEFAULT_PASSWORD);
	}

	public Session(String host, String port, String username, String password) {
		this(host, Integer.parseInt(port), username, password);
	}

	public Session(String host, int port, String username, String password) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;
		this.isClosed = true;
	}

	public synchronized void openSession() throws SessionException, TTransportException {
		if (!isClosed)  {
			return;
		}

		transport = new TSocket(host, port);
		if (!transport.isOpen()) {
			try {
				transport.open();
			} catch (TTransportException e) {
				throw new SessionException(e);
			}
		}

		client = new IService.Client(new TBinaryProtocol(transport));

		OpenSessionReq req = new OpenSessionReq();
		req.setUsername(username);
		req.setPassword(password);

		try {
			OpenSessionResp resp = client.openSession(req);

			sessionId = resp.getSessionId();
		} catch (TException e) {
			transport.close();
			throw new SessionException(e);
		}

		isClosed = false;
	}

	public synchronized void closeSession() throws SessionException {
		if (isClosed) {
			return;
		}

		CloseSessionReq req = new CloseSessionReq(sessionId);
		try {
			client.closeSession(req);
		} catch (TException e) {
			throw new SessionException(e);
		} finally {
			isClosed = true;
			if (transport != null) {
				transport.close();
			}
		}
	}

	public void createDatabase(String databaseName) throws SessionException,
			ExecutionException {
		CreateDatabaseReq req = new CreateDatabaseReq(sessionId, databaseName);

		try {
			RpcUtils.verifySuccess(client.createDatabase(req));
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public void dropDatabase(String databaseName) throws SessionException, ExecutionException {
		DropDatabaseReq req = new DropDatabaseReq(sessionId, databaseName);

		try {
			RpcUtils.verifySuccess(client.dropDatabase(req));
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public void addStorageEngine(String ip, int port, StorageEngineType type, Map<String, String> extraParams) throws SessionException, ExecutionException {
		AddStorageEngineReq req = new AddStorageEngineReq(sessionId, ip, port, type, extraParams);

		try {
			RpcUtils.verifySuccess(client.addStorageEngine(req));
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public void addColumn(String path) throws ExecutionException, SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(path);
		addColumns(paths);
	}

	public void addColumns(List<String> paths) throws SessionException, ExecutionException {
		AddColumnsReq req = new AddColumnsReq(sessionId, paths);

		try {
			RpcUtils.verifySuccess(client.addColumns(req));
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public void addColumns(List<String> paths, List<Map<String, String>> attributes) throws SessionException, ExecutionException {
		AddColumnsReq req = new AddColumnsReq(sessionId, paths);
		req.setAttributesList(attributes);

		try {
			RpcUtils.verifySuccess(client.addColumns(req));
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public void deleteColumn(String path) throws SessionException,
			ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(path);
		deleteColumns(paths);
	}

	public void deleteColumns(List<String> paths) throws SessionException, ExecutionException {
		DeleteColumnsReq req = new DeleteColumnsReq(sessionId, paths);

		try {
			RpcUtils.verifySuccess(client.deleteColumns(req));
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public void insertColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
									List<DataType> dataTypeList, List<Map<String, String>> attributesList) throws SessionException, ExecutionException {
		if (paths.size() != valuesList.length || paths.size() != dataTypeList.size()) {
			logger.error("The sizes of paths, valuesList and dataTypeList should be equal.");
			return;
		}
		if (attributesList != null && paths.size() != attributesList.size()) {
			logger.error("The sizes of paths, valuesList, dataTypeList and attributesList should be equal.");
			return;
		}

		Integer[] index = new Integer[timestamps.length];
		for (int i = 0; i < timestamps.length; i++) {
			index[i] = i;
		}
		Arrays.sort(index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(timestamps))::get));
		Arrays.sort(timestamps);
		for (int i = 0; i < valuesList.length; i++) {
			Object[] values = new Object[index.length];
			for (int j = 0; j < index.length; j++) {
				values[j] = ((Object[]) valuesList[i])[index[j]];
			}
			valuesList[i] = values;
		}

		InsertColumnRecordsReq req = new InsertColumnRecordsReq();
		req.setSessionId(sessionId);
		req.setPaths(paths);
		req.setTimestamps(getByteArrayFromLongArray(timestamps));
		req.setValuesList(getByteBufferByDataType(valuesList, dataTypeList));
		req.setDataTypeList(dataTypeList);
		req.setAttributesList(attributesList);

		try {
			RpcUtils.verifySuccess(client.insertColumnRecords(req));
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public void insertRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
								 List<DataType> dataTypeList, List<Map<String, String>> attributesList) throws SessionException, ExecutionException {
		if (paths.size() != valuesList.length || paths.size() != dataTypeList.size()) {
			logger.error("The sizes of paths and dataTypeList should be equal.");
			return;
		}
		if (timestamps.length != valuesList.length) {
			logger.error("The sizes of timestamps and valuesList should be equal.");
			return;
		}
		if (attributesList != null && paths.size() != attributesList.size()) {
			logger.error("The sizes of paths, valuesList, dataTypeList and attributesList should be equal.");
			return;
		}

		List<ByteBuffer> valueBufferList = new ArrayList<>();
		List<ByteBuffer> bitmapBufferList = new ArrayList<>();
		for (int i = 0; i < timestamps.length; i++) {
			Object[] values = (Object[]) valuesList[i];
			if (values.length != paths.size()) {
				logger.error("The sizes of paths and the element of valuesList should be equal.");
				return;
			}
			valueBufferList.add(getByteBuffer(values, dataTypeList));
			Bitmap bitmap = new Bitmap(values.length);
			for (int j = 0; j < values.length; j++) {
				if (values[j] != null) {
					bitmap.mark(j);
				}
			}
			bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
		}

		InsertRowRecordsReq req = new InsertRowRecordsReq();
		req.setSessionId(sessionId);
		req.setPaths(paths);
		req.setTimestamps(getByteArrayFromLongArray(timestamps));
		req.setDataTypeList(dataTypeList);
		req.setAttributesList(attributesList);
		req.setValuesList(valueBufferList);
		req.setBitmapList(bitmapBufferList);

		try {
			RpcUtils.verifySuccess(client.insertRowRecords(req));
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public void deleteDataInColumns(String path, long startTime, long endTime) throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(path);
		deleteDataInColumns(paths, startTime, endTime);
	}

	public void deleteDataInColumns(List<String> paths, long startTime, long endTime) throws SessionException {
		DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(sessionId, paths, startTime, endTime);

		try {
			client.deleteDataInColumns(req);
		} catch (TException e) {
			throw new SessionException(e);
		}
	}

	public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime)
			throws SessionException {
		QueryDataReq req = new QueryDataReq(sessionId, paths, startTime, endTime);

		QueryDataResp resp;
		try {
			resp = client.queryData(req);
		} catch (TException e) {
			throw new SessionException(e);
		}
		return new SessionQueryDataSet(resp);
	}

	public SessionAggregateQueryDataSet aggregateQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType)
			throws SessionException {
		AggregateQueryReq req = new AggregateQueryReq(sessionId, paths, startTime, endTime, aggregateType);

		AggregateQueryResp resp;
		try {
			resp = client.aggregateQuery(req);
		} catch (TException e) {
			throw new SessionException(e);
		}
		return new SessionAggregateQueryDataSet(resp);
	}
}
