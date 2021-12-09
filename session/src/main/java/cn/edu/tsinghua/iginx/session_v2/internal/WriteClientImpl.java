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
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.write.Point;
import cn.edu.tsinghua.iginx.session_v2.write.Record;
import cn.edu.tsinghua.iginx.session_v2.write.Table;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedColumnRecordsReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

public class WriteClientImpl extends AbstractFunctionClient implements WriteClient {

    private static final Logger logger = LoggerFactory.getLogger(WriteClientImpl.class);

    public WriteClientImpl(IginXClientImpl iginXClient) {
        super(iginXClient);
    }

    @Override
    public void writePoint(Point point){
        writePoints(Collections.singletonList(point));
    }

    @Override
    public void writePoints(List<Point> points) {
        SortedMap<String, DataType> measurementMap = new TreeMap<>();
        List<Long> timestampList = new ArrayList<>();
        for (Point point: points) {
            String measurement = point.getMeasurement();
            DataType dataType = point.getDataType();
            if (measurementMap.getOrDefault(measurement, dataType) != dataType) {
                throw new IllegalArgumentException("measurement " + measurement + " has multi data type, which is invalid.");
            }
            measurementMap.putIfAbsent(measurement, dataType);
            timestampList.add(point.getTimestamp());
        }
        List<String> measurements = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        Map<String, Integer> measurementIndexMap = new HashMap<>();
        int index = 0;
        for (String measurement: measurementMap.keySet()) {
            measurementIndexMap.put(measurement, index);
            index++;
            measurements.add(measurement);
            dataTypeList.add(measurementMap.get(measurement));
        }

        timestampList.sort(Long::compareTo);
        long[] timestamps = timestampList.stream().mapToLong(e -> e).toArray();
        Map<Long, Integer> timestampIndexMap = new HashMap<>();
        for (int i = 0; i < timestamps.length; i++) {
            timestampIndexMap.put(timestamps[i], i);
        }

        Object[] valuesList = new Object[measurements.size()];
        for (int i = 0; i < valuesList.length; i++) {
            valuesList[i] = new Object[timestamps.length];
        }
        for (Point point: points) {
            String measurement = point.getMeasurement();
            long timestamp = point.getTimestamp();
            int measurementIndex = measurementIndexMap.get(measurement);
            int timestampIndex = timestampIndexMap.get(timestamp);
            ((Object[]) valuesList[measurementIndex])[timestampIndex] = point.getValue();
        }
        writeColumnData(measurements, timestamps, valuesList, dataTypeList);
    }

    @Override
    public void writeRecord(Record record) {

    }

    @Override
    public void writeRecords(List<Record> records) {

    }

    @Override
    public <M> void writeMeasurement(M measurement) {

    }

    @Override
    public <M> void writeMeasurements(List<M> measurements) {

    }

    @Override
    public void writeTable(Table table) {

    }

    private void writeColumnData(List<String> paths, long[] timestamps, Object[] valuesList,
                                              List<DataType> dataTypeList) {
        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (int i = 0; i < valuesList.length; i++) {
            Object[] values = (Object[]) valuesList[i];

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
        req.setAttributesList(new ArrayList<>());

        try {
            lock.lock();
            Status status = client.insertNonAlignedColumnRecords(req);
            RpcUtils.verifySuccess(status);
        } catch (TException | ExecutionException e) {
            logger.error("insert data failure: ", e);
        } finally {
            lock.unlock();
        }
    }

}
