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
package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
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
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.DropDatabaseReq;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

public class RestSession {
    private static final Logger logger = LoggerFactory.getLogger(RestSession.class);
    private final ReadWriteLock lock;
    private IginxWorker client;
    private long sessionId;
    private boolean isClosed;
    private int redirectTimes;


    public RestSession() {
        this.isClosed = true;
        this.redirectTimes = 0;
        this.lock = new ReentrantReadWriteLock();

    }


    private OpenSessionResp tryOpenSession() {
        client = IginxWorker.getInstance();
        OpenSessionReq req = new OpenSessionReq();
        return client.openSession(req);
    }

    private void tryCloseSession() {
        CloseSessionReq req = new CloseSessionReq(sessionId);
        client.closeSession(req);
    }

    public synchronized void openSession() throws SessionException {
        if (!isClosed) {
            return;
        }

        do {
            OpenSessionResp resp = tryOpenSession();

            if (RpcUtils.verifyNoRedirect(resp.status)) {
                sessionId = resp.getSessionId();
                break;
            }


            String[] targetAddress = resp.status.getMessage().split(":");
            if (targetAddress.length != 2) {
                throw new SessionException("unexpected redirect address " + resp.status.getMessage());
            }
            logger.info("当前请求将被重定向到：" + resp.status.getMessage());
            redirectTimes += 1;

        } while (redirectTimes <= Constants.MAX_REDIRECT_TIME);

        if (redirectTimes > Constants.MAX_REDIRECT_TIME) {
            throw new SessionException("重定向次数过多！");
        }
        redirectTimes = 0;

        isClosed = false;
    }

    public synchronized void closeSession() {
        if (isClosed) {
            return;
        }
        try {
            tryCloseSession();
        } finally {
            isClosed = true;
        }
    }

    private synchronized boolean checkRedirect(Status status) {
        return false;
    }


    public void createDatabase(String databaseName) throws ExecutionException {
        CreateDatabaseReq req = new CreateDatabaseReq(sessionId, databaseName);

        Status status;
        do {
            lock.readLock().lock();
            try {
                status = client.createDatabase(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(status));
        RpcUtils.verifySuccess(status);

    }

    public void dropDatabase(String databaseName) throws ExecutionException {
        DropDatabaseReq req = new DropDatabaseReq(sessionId, databaseName);

        Status status;
        do {
            lock.readLock().lock();
            try {
                status = client.dropDatabase(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(status));
        RpcUtils.verifySuccess(status);
    }

    public void addStorageEngine(String ip, int port, StorageEngineType type, Map<String, String> extraParams) throws ExecutionException {
        AddStorageEngineReq req = new AddStorageEngineReq(sessionId, ip, port, type, extraParams);

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
    }

    public void addColumn(String path) throws ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(path);
        addColumns(paths);
    }

    public void addColumns(List<String> paths) throws ExecutionException {
        AddColumnsReq req = new AddColumnsReq(sessionId, paths);

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
    }

    public void addColumns(List<String> paths, List<Map<String, String>> attributes) throws ExecutionException {
        AddColumnsReq req = new AddColumnsReq(sessionId, paths);
        req.setAttributesList(attributes);

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
    }

    public void deleteColumn(String path) throws ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add(path);
        deleteColumns(paths);
    }

    public void deleteColumns(List<String> paths) throws ExecutionException {
        DeleteColumnsReq req = new DeleteColumnsReq(sessionId, paths);

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
    }

    public void insertColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                    List<DataType> dataTypeList, List<Map<String, String>> attributesList) throws ExecutionException {
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

        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (int i = 0; i < valuesList.length; i++) {
            Object[] values = (Object[]) valuesList[i];
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
    }

    public void insertRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                 List<DataType> dataTypeList, List<Map<String, String>> attributesList) throws ExecutionException {
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
    }

    public void deleteDataInColumn(String path, long startTime, long endTime) {
        List<String> paths = new ArrayList<>();
        paths.add(path);
        deleteDataInColumns(paths, startTime, endTime);
    }

    public void deleteDataInColumns(List<String> paths, long startTime, long endTime) {
        DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(sessionId, paths, startTime, endTime);

        Status status;
        do {
            lock.readLock().lock();
            try {
                status = client.deleteDataInColumns(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(status));
    }

    public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime) {
        if (paths.isEmpty() || startTime > endTime) {
            logger.error("Invalid query request!");
            return null;
        }
        QueryDataReq req = new QueryDataReq(sessionId, paths, startTime, endTime);

        QueryDataResp resp;

        do {
            lock.readLock().lock();
            try {
                resp = client.queryData(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(resp.status));

        return new SessionQueryDataSet(resp);
    }

    public SessionQueryDataSet valueFilterQuery(List<String> paths, long startTime, long endTime, String booleanExpression)
            throws SessionException {
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
        } catch (Exception e) {
            throw new SessionException(e);
        }

        return new SessionQueryDataSet(resp);
    }

    public SessionAggregateQueryDataSet aggregateQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType) {
        AggregateQueryReq req = new AggregateQueryReq(sessionId, paths, startTime, endTime, aggregateType);

        AggregateQueryResp resp;
        do {
            lock.readLock().lock();
            try {
                resp = client.aggregateQuery(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(resp.status));

        return new SessionAggregateQueryDataSet(resp, aggregateType);
    }

    public SessionQueryDataSet downsampleQuery(List<String> paths, long startTime, long endTime, AggregateType aggregateType, long precision) {
        DownsampleQueryReq req = new DownsampleQueryReq(sessionId, paths, startTime, endTime,
                aggregateType, precision);

        DownsampleQueryResp resp;

        do {
            lock.readLock().lock();
            try {
                resp = client.downsampleQuery(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(resp.status));

        return new SessionQueryDataSet(resp);
    }

}
