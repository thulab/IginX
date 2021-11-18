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
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.InsertRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import cn.edu.tsinghua.iginx.thrift.OpenSessionResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

public class RestSession {
    private static final Logger logger = LoggerFactory.getLogger(RestSession.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private IginxWorker client;
    private long sessionId;
    private boolean isClosed;
    private int redirectTimes;
    private final String username;
    private final String password;

    public RestSession() {
        this.isClosed = true;
        this.redirectTimes = 0;
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



    public void deleteColumns(List<String> paths) throws ExecutionException {
        DeleteColumnsReq req = new DeleteColumnsReq(sessionId, paths);

        Status status;
        do {
                status = client.deleteColumns(req);
        } while(checkRedirect(status));
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
        if (attributesList != null) {
            for (Integer i : index) {
                sortedAttributesList.add(attributesList.get(i));
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

        InsertRowRecordsReq req = new InsertRowRecordsReq();
        req.setSessionId(sessionId);
        req.setPaths(paths);
        req.setTimestamps(getByteArrayFromLongArray(timestamps));
        req.setValuesList(valueBufferList);
        req.setBitmapList(bitmapBufferList);
        req.setDataTypeList(sortedDataTypeList);
        req.setAttributesList(sortedAttributesList);

        Status status;
        do {
            status = client.insertRowRecords(req);
        } while(checkRedirect(status));
        RpcUtils.verifySuccess(status);
    }


    public void deleteDataInColumns(List<String> paths, long startTime, long endTime) {
        DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(sessionId, paths, startTime, endTime);

        Status status;
        do {
                status = client.deleteDataInColumns(req);
        } while(checkRedirect(status));
    }

    public SessionQueryDataSet queryData(List<String> paths, long startTime, long endTime) {
        if (paths.isEmpty() || startTime > endTime) {
            logger.error("Invalid query request!");
            return null;
        }
        QueryDataReq req = new QueryDataReq(sessionId, mergeAndSortPaths(paths), startTime, endTime);

        QueryDataResp resp;

        do {
                resp = client.queryData(req);
        } while(checkRedirect(resp.status));

        return new SessionQueryDataSet(resp);
    }

    // 适用于查询类请求和删除类请求，因为其 paths 可能带有 *
    private List<String> mergeAndSortPaths(List<String> paths) {
        if (paths.stream().anyMatch(x -> x.equals("*"))) {
            List<String> tempPaths = new ArrayList<>();
            tempPaths.add("*");
            return tempPaths;
        }
        List<String> prefixes = paths.stream().filter(x -> x.contains("*")).map(x -> x.substring(0, x.indexOf("*"))).collect(Collectors.toList());
        if (prefixes.isEmpty()) {
            Collections.sort(paths);
            return paths;
        }
        List<String> mergedPaths = new ArrayList<>();
        for (String path : paths) {
            if (!path.contains("*")) {
                boolean skip = false;
                for (String prefix : prefixes) {
                    if (path.startsWith(prefix)) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }
            }
            mergedPaths.add(path);
        }
        mergedPaths.sort(String::compareTo);
        return mergedPaths;
    }

}
