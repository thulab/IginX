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
import cn.edu.tsinghua.iginx.thrift.DataType;
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
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryReq;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryResp;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

public class Session {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);
    private final String username;
    private final String password;
    private final ReadWriteLock lock;
    private String host;
    private int port;
    private IService.Iface client;
    private long sessionId;
    private TTransport transport;
    private boolean isClosed;
    private int redirectTimes;

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
        this.redirectTimes = 0;
        this.lock = new ReentrantReadWriteLock();
    }

    private synchronized boolean checkRedirect(Status status) throws SessionException, TException {
        if (RpcUtils.verifyNoRedirect(status)) {
            redirectTimes = 0;
            return false;
        }

        redirectTimes += 1;
        if (redirectTimes > Constants.MAX_REDIRECT_TIME) {
            throw new SessionException("重定向次数过多！");
        }

        lock.writeLock().lock();

        try {
            tryCloseSession();

            while (redirectTimes <= Constants.MAX_REDIRECT_TIME) {

                String[] targetAddress = status.getMessage().split(":");
                if (targetAddress.length != 2) {
                    throw new SessionException("unexpected redirect address " + status.getMessage());
                }
                logger.info("当前请求将被重定向到：" + status.getMessage());
                this.host = targetAddress[0];
                this.port = Integer.parseInt(targetAddress[1]);

                OpenSessionResp resp = tryOpenSession();

                if (RpcUtils.verifyNoRedirect(resp.status)) {
                    sessionId = resp.getSessionId();
                    break;
                }

                status = resp.status;

                redirectTimes += 1;
            }

            if (redirectTimes > Constants.MAX_REDIRECT_TIME) {
                throw new SessionException("重定向次数过多！");
            }
        } finally {
            lock.writeLock().unlock();
        }

        return true;
    }

    private OpenSessionResp tryOpenSession() throws SessionException, TException {
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

        return client.openSession(req);
    }

    private void tryCloseSession() throws SessionException {
        CloseSessionReq req = new CloseSessionReq(sessionId);
        try {
            client.closeSession(req);
        } catch (TException e) {
            throw new SessionException(e);
        } finally {
            if (transport != null) {
                transport.close();
            }
        }
    }

    public synchronized void openSession() throws SessionException {
        if (!isClosed) {
            return;
        }

        try {
            do {
                OpenSessionResp resp = tryOpenSession();

                if (RpcUtils.verifyNoRedirect(resp.status)) {
                    sessionId = resp.getSessionId();
                    break;
                }

                transport.close();

                String[] targetAddress = resp.status.getMessage().split(":");
                if (targetAddress.length != 2) {
                    throw new SessionException("unexpected redirect address " + resp.status.getMessage());
                }
                logger.info("当前请求将被重定向到：" + resp.status.getMessage());

                this.host = targetAddress[0];
                this.port = Integer.parseInt(targetAddress[1]);
                redirectTimes += 1;

            } while (redirectTimes <= Constants.MAX_REDIRECT_TIME);

            if (redirectTimes > Constants.MAX_REDIRECT_TIME) {
                throw new SessionException("重定向次数过多！");
            }
            redirectTimes = 0;

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
        try {
            tryCloseSession();
        } finally {
            isClosed = true;
        }
    }

    public void addStorageEngine(String ip, int port, StorageEngineType type, Map<String, String> extraParams) throws SessionException, ExecutionException {
        AddStorageEngineReq req = new AddStorageEngineReq(sessionId, ip, port, type, extraParams);

        try {
            Status status;
            do {
                lock.readLock().lock();
                try {
                    status = client.addStorageEngine(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(status));
            RpcUtils.verifySuccess(status);
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
            Status status;
            do {
                lock.readLock().lock();
                try {
                    status = client.addColumns(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(status));
            RpcUtils.verifySuccess(status);
        } catch (TException e) {
            throw new SessionException(e);
        }
    }

    public void addColumns(List<String> paths, List<Map<String, String>> attributes) throws SessionException, ExecutionException {
        AddColumnsReq req = new AddColumnsReq(sessionId, paths);
        req.setAttributesList(attributes);

        try {
            Status status;
            do {
                lock.readLock().lock();
                try {
                    status = client.addColumns(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(status));
            RpcUtils.verifySuccess(status);
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
            Status status;
            do {
                lock.readLock().lock();
                try {
                    status = client.deleteColumns(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(status));
            RpcUtils.verifySuccess(status);
        } catch (TException e) {
            throw new SessionException(e);
        }
    }

    public void insertColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                    List<DataType> dataTypeList, List<Map<String, String>> attributesList) throws SessionException, ExecutionException {
        if (paths.isEmpty() || timestamps.length == 0 || valuesList.length == 0 || dataTypeList.isEmpty()) {
            logger.error("Invalid insert request!");
            return;
        }
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

        // TODO sort dataTypeList and attributesList
        index = new Integer[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            index[i] = i;
        }
        Arrays.sort(index, Comparator.comparing(paths::get));
        Collections.sort(paths);
        Object[] sortedValuesList = new Object[valuesList.length];
        for (int i = 0; i < valuesList.length; i++) {
            sortedValuesList[i] = valuesList[index[i]];
        }

        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (int i = 0; i < valuesList.length; i++) {
            Object[] values = (Object[]) sortedValuesList[i];
            if (values.length != timestamps.length) {
                logger.error("The sizes of timestamps and the element of valuesList should be equal.");
                return;
            }
            valueBufferList.add(ByteUtils.getColumnByteBuffer(values, dataTypeList.get(i)));
            Bitmap bitmap = new Bitmap(timestamps.length);
            for (int j = 0; j < timestamps.length; j++) {
                if (values[j] != null) {
                    bitmap.mark(j);
                }
            }
            bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
        }

        InsertColumnRecordsReq req = new InsertColumnRecordsReq();
        req.setSessionId(sessionId);
        req.setPaths(paths);
        req.setTimestamps(getByteArrayFromLongArray(timestamps));
        req.setValuesList(valueBufferList);
        req.setBitmapList(bitmapBufferList);
        req.setDataTypeList(dataTypeList);
        req.setAttributesList(attributesList);

        try {
            Status status;
            do {
                lock.readLock().lock();
                try {
                    status = client.insertColumnRecords(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(status));
            RpcUtils.verifySuccess(status);
        } catch (TException e) {
            throw new SessionException(e);
        }
    }

    public void insertRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                 List<DataType> dataTypeList, List<Map<String, String>> attributesList) throws SessionException, ExecutionException {
        if (paths.isEmpty() || timestamps.length == 0 || valuesList.length == 0 || dataTypeList.isEmpty()) {
            logger.error("Invalid insert request!");
            return;
        }
        if (paths.size() != dataTypeList.size()) {
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

        Integer[] index = new Integer[timestamps.length];
        for (int i = 0; i < timestamps.length; i++) {
            index[i] = i;
        }
        Arrays.sort(index, Comparator.comparingLong(Arrays.asList(ArrayUtils.toObject(timestamps))::get));
        Arrays.sort(timestamps);
        Object[] sortedValuesList = new Object[valuesList.length];
        for (int i = 0; i < valuesList.length; i++) {
            sortedValuesList[i] = valuesList[index[i]];
        }

        // TODO sort dataTypeList and attributesList
        index = new Integer[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            index[i] = i;
        }
        Arrays.sort(index, Comparator.comparing(paths::get));
        Collections.sort(paths);
        for (int i = 0; i < sortedValuesList.length; i++) {
            Object[] values = new Object[index.length];
            for (int j = 0; j < index.length; j++) {
                values[j] = ((Object[]) sortedValuesList[i])[index[j]];
            }
            sortedValuesList[i] = values;
        }

        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (int i = 0; i < timestamps.length; i++) {
            Object[] values = (Object[]) sortedValuesList[i];
            if (values.length != paths.size()) {
                logger.error("The sizes of paths and the element of valuesList should be equal.");
                return;
            }
            valueBufferList.add(ByteUtils.getRowByteBuffer(values, dataTypeList));
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
        req.setValuesList(valueBufferList);
        req.setBitmapList(bitmapBufferList);
        req.setDataTypeList(dataTypeList);
        req.setAttributesList(attributesList);

        try {
            Status status;
            do {
                lock.readLock().lock();
                try {
                    status = client.insertRowRecords(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(status));
            RpcUtils.verifySuccess(status);
        } catch (TException e) {
            throw new SessionException(e);
        }
    }

    public void deleteDataInColumn(String path, long startTime, long endTime) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(path);
        deleteDataInColumns(paths, startTime, endTime);
    }

    public void deleteDataInColumns(List<String> paths, long startTime, long endTime) throws SessionException, ExecutionException {
        DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(sessionId, paths, startTime, endTime);

        try {
            Status status;
            do {
                lock.readLock().lock();
                try {
                    status = client.deleteDataInColumns(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(status));
            RpcUtils.verifySuccess(status);
        } catch (TException e) {
            throw new SessionException(e);
        }
    }

    public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime)
            throws SessionException, ExecutionException {
        if (paths.isEmpty() || startTime > endTime) {
            logger.error("Invalid query request!");
            return null;
        }
        QueryDataReq req = new QueryDataReq(sessionId, paths, startTime, endTime);

        QueryDataResp resp;

        try {
            do {
                lock.readLock().lock();
                try {
                    resp = client.queryData(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(resp.status));
            RpcUtils.verifySuccess(resp.status);
        } catch (TException e) {
            throw new SessionException(e);
        }

        return new SessionQueryDataSet(resp);
    }

    public SessionQueryDataSet valueFilterQuery(List<String> paths, long startTime, long endTime, String booleanExpression)
            throws SessionException, ExecutionException {
        if (paths.isEmpty() || startTime > endTime) {
            logger.error("Invalid query request!");
            return null;
        }
        ValueFilterQueryReq req = new ValueFilterQueryReq(sessionId, paths, startTime, endTime, booleanExpression);

        ValueFilterQueryResp resp;

        try {
            do {
                lock.readLock().lock();
                try {
                    resp = client.valueFilterQuery(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(resp.status));
            RpcUtils.verifySuccess(resp.status);
        } catch (TException e) {
            throw new SessionException(e);
        }

        return new SessionQueryDataSet(resp);
    }

    public SessionAggregateQueryDataSet aggregateQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType)
            throws SessionException, ExecutionException {
        AggregateQueryReq req = new AggregateQueryReq(sessionId, paths, startTime, endTime, aggregateType);

        AggregateQueryResp resp;
        try {
            do {
                lock.readLock().lock();
                try {
                    resp = client.aggregateQuery(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(resp.status));
            RpcUtils.verifySuccess(resp.status);
        } catch (TException e) {
            throw new SessionException(e);
        }

        return new SessionAggregateQueryDataSet(resp, aggregateType);
    }

    public SessionQueryDataSet downsampleQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision) throws SessionException, ExecutionException {
        DownsampleQueryReq req = new DownsampleQueryReq(sessionId, paths, startTime, endTime,
                aggregateType, precision);

        DownsampleQueryResp resp;

        try {
            do {
                lock.readLock().lock();
                try {
                    resp = client.downsampleQuery(req);
                } finally {
                    lock.readLock().unlock();
                }
            } while (checkRedirect(resp.status));
            RpcUtils.verifySuccess(resp.status);
        } catch (TException e) {
            throw new SessionException(e);
        }

        return new SessionQueryDataSet(resp);
    }

}
