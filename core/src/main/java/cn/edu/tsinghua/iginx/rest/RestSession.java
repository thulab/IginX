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

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import cn.edu.tsinghua.iginx.utils.TimeUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

public class RestSession {
    private static final Logger logger = LoggerFactory.getLogger(RestSession.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private final ReadWriteLock lock;
    private IginxWorker client;
    private long sessionId;
    private boolean isClosed;
    private int redirectTimes;
    private final String username;
    private final String password;

    public RestSession() {
        this.isClosed = true;
        this.redirectTimes = 0;
        this.lock = new ReentrantReadWriteLock();
        this.username = config.getUsername();
        this.password = config.getPassword();
    }


    private OpenSessionResp tryOpenSession() {
        client = IginxWorker.getInstance();
        OpenSessionReq req = new OpenSessionReq();
        req.username = this.username;
        req.password = this.password;
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

    public void addStorageEngine(String ip, int port, String type, Map<String, String> extraParams) throws ExecutionException {
        StorageEngine storageEngine = new StorageEngine(ip, port, type, extraParams);
        AddStorageEnginesReq req = new AddStorageEnginesReq(sessionId, Collections.singletonList(storageEngine));

        Status status;
        do {
            lock.readLock().lock();
            try {
                status = client.addStorageEngines(req);
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

    public void insertNonAlignedColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                              List<DataType> dataTypeList, List<Map<String, String>> tagsList) throws ExecutionException {
        insertNonAlignedColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList, TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
    }

    public void insertNonAlignedColumnRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                              List<DataType> dataTypeList, List<Map<String, String>> tagsList, TimePrecision timePrecision) throws ExecutionException {
        if (paths.isEmpty() || timestamps.length == 0 || valuesList.length == 0 || dataTypeList.isEmpty()) {
            logger.error("Invalid insert request!");
            return;
        }
        if (paths.size() != valuesList.length || paths.size() != dataTypeList.size()) {
            logger.error("The sizes of paths, valuesList and dataTypeList should be equal.");
            return;
        }
        if (tagsList != null && paths.size() != tagsList.size()) {
            logger.error("The sizes of paths, valuesList, dataTypeList and tagsList should be equal.");
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

        InsertNonAlignedColumnRecordsReq req = new InsertNonAlignedColumnRecordsReq();
        req.setSessionId(sessionId);
        req.setPaths(paths);
        req.setTimestamps(getByteArrayFromLongArray(timestamps));
        req.setValuesList(valueBufferList);
        req.setBitmapList(bitmapBufferList);
        req.setDataTypeList(dataTypeList);
        req.setTagsList(tagsList);
        req.setTimePrecision(timePrecision);

        Status status;
        do {
            lock.readLock().lock();
            try {
                status = client.insertNonAlignedColumnRecords(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(status));
        RpcUtils.verifySuccess(status);
    }

    public void insertNonAlignedRowRecords(List<String> paths, long[] timestamps, Object[] valuesList,
                                           List<DataType> dataTypeList, List<Map<String, String>> tagsList, TimePrecision timePrecision) throws ExecutionException {
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
        if (tagsList != null && paths.size() != tagsList.size()) {
            logger.error("The sizes of paths, valuesList, dataTypeList and tagsList should be equal.");
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

        index = new Integer[paths.size()];
        for (int i = 0; i < paths.size(); i++) {
            index[i] = i;
        }
        Arrays.sort(index, Comparator.comparing(paths::get));
        Collections.sort(paths);
        List<DataType> sortedDataTypeList = new ArrayList<>();
        List<Map<String, String>> sortedAttributesList = new ArrayList<>();
        for (int i = 0; i < sortedValuesList.length; i++) {
            Object[] values = new Object[index.length];
            for (int j = 0; j < index.length; j++) {
                values[j] = ((Object[]) sortedValuesList[i])[index[j]];
            }
            sortedValuesList[i] = values;
        }
        for (Integer i : index) {
            sortedDataTypeList.add(dataTypeList.get(i));
        }
        if (tagsList != null) {
            for (Integer i : index) {
                sortedAttributesList.add(tagsList.get(i));
            }
        }

        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (int i = 0; i < timestamps.length; i++) {
            Object[] values = (Object[]) sortedValuesList[i];
            if (values.length != paths.size()) {
                logger.error("The sizes of paths and the element of valuesList should be equal.");
                return;
            }
            valueBufferList.add(ByteUtils.getRowByteBuffer(values, sortedDataTypeList));
            Bitmap bitmap = new Bitmap(values.length);
            for (int j = 0; j < values.length; j++) {
                if (values[j] != null) {
                    bitmap.mark(j);
                }
            }
            bitmapBufferList.add(ByteBuffer.wrap(bitmap.getBytes()));
        }

        InsertNonAlignedRowRecordsReq req = new InsertNonAlignedRowRecordsReq();
        req.setSessionId(sessionId);
        req.setPaths(paths);
        req.setTimestamps(getByteArrayFromLongArray(timestamps));
        req.setValuesList(valueBufferList);
        req.setBitmapList(bitmapBufferList);
        req.setDataTypeList(sortedDataTypeList);
        req.setTagsList(sortedAttributesList);
        req.setTimePrecision(timePrecision);

        Status status;
        do {
            lock.readLock().lock();
            try {
                status = client.insertNonAlignedRowRecords(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(status));
        RpcUtils.verifySuccess(status);
    }

    public void deleteDataInColumn(String path, Map<String, List<String>> tagList, long startTime, long endTime) {
        List<String> paths = new ArrayList<>();
        paths.add(path);
        deleteDataInColumns(paths, tagList, startTime, endTime);
    }

    public void deleteDataInColumns(List<String> paths, Map<String, List<String>> tagList, long startTime, long endTime) {
        deleteDataInColumns(paths, tagList, startTime, endTime, TimeUtils.DEFAULT_TIMESTAMP_PRECISION);
    }

    public void deleteDataInColumns(List<String> paths, Map<String, List<String>> tagList, long startTime, long endTime, TimePrecision timePrecision) {
        DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(sessionId, paths, startTime, endTime);
        if (!tagList.isEmpty())//LHZ这里要全部将size改为这个empty的判断
            req.setTagsList(tagList);
        req.setTimePrecision(timePrecision);

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

    public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime, Map<String, List<String>> tagList) {
        return queryData(paths, startTime, endTime, tagList);
    }

    public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime, Map<String, List<String>> tagList, TimePrecision timePrecision) {
        if (paths.isEmpty() || startTime > endTime) {
            logger.error("Invalid query request!");
            return null;
        }
        QueryDataReq req = new QueryDataReq(sessionId, paths, startTime, endTime);
        if(tagList.size()!=0)
            req.setTagsList(tagList);
        req.setTimePrecision(timePrecision);

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

    public SessionAggregateQueryDataSet aggregateQuery(List<String> paths, long startTime, long endTime, Map<String, List<String>> tagList, AggregateType aggregateType, TimePrecision timePrecision) {
        AggregateQueryReq req = new AggregateQueryReq(sessionId, paths, startTime, endTime, aggregateType);
        req.setTagsList(tagList);
        req.setTimePrecision(timePrecision);
        
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

    public SessionQueryDataSet downsampleQuery(List<String> paths, Map<String, List<String>> tagList, long startTime, long endTime, AggregateType aggregateType, long precision, TimePrecision timePrecision) {
        DownsampleQueryReq req = new DownsampleQueryReq(sessionId, paths, startTime, endTime, aggregateType, precision);
        req.setTagsList(tagList);
        req.setTimePrecision(timePrecision);

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

    public SessionQueryDataSet showTimeSeries() {
        ShowColumnsReq req = new ShowColumnsReq(sessionId);

        ShowColumnsResp resp;

        do {
            lock.readLock().lock();
            try {
                resp = client.showColumns(req);
            } finally {
                lock.readLock().unlock();
            }
        } while (checkRedirect(resp.status));

        return new SessionQueryDataSet(resp);//这个初始化方法没有定义LHZ
    }

}
