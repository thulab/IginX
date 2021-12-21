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
import cn.edu.tsinghua.iginx.thrift.InsertNonAlignedRowRecordsReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getByteArrayFromLongArray;

public class WriteClientImpl extends AbstractFunctionClient implements WriteClient {

    private static final Logger logger = LoggerFactory.getLogger(WriteClientImpl.class);

    private final MeasurementMapper measurementMapper;

    public WriteClientImpl(IginXClientImpl iginXClient) {
        super(iginXClient);
        measurementMapper = new MeasurementMapper();
    }

    @Override
    public void writePoint(Point point){
        writePoints(Collections.singletonList(point));
    }

    @Override
    public void writePoints(List<Point> points) {
        SortedMap<String, DataType> measurementMap = new TreeMap<>();
        Set<Long> timestampSet = new HashSet<>();
        for (Point point: points) {
            String measurement = point.getMeasurement();
            DataType dataType = point.getDataType();
            if (measurementMap.getOrDefault(measurement, dataType) != dataType) {
                throw new IllegalArgumentException("measurement " + measurement + " has multi data type, which is invalid.");
            }
            measurementMap.putIfAbsent(measurement, dataType);
            timestampSet.add(point.getTimestamp());
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

        long[] timestamps = timestampSet.stream().sorted().mapToLong(e -> e).toArray();
        Map<Long, Integer> timestampIndexMap = new HashMap<>();
        for (int i = 0; i < timestamps.length; i++) {
            timestampIndexMap.put(timestamps[i], i);
        }

        Object[][] valuesList = new Object[measurements.size()][];
        for (int i = 0; i < valuesList.length; i++) {
            valuesList[i] = new Object[timestamps.length];
        }
        for (Point point: points) {
            String measurement = point.getMeasurement();
            long timestamp = point.getTimestamp();
            int measurementIndex = measurementIndexMap.get(measurement);
            int timestampIndex = timestampIndexMap.get(timestamp);
            valuesList[measurementIndex][timestampIndex] = point.getValue();
        }
        writeColumnData(measurements, timestamps, valuesList, dataTypeList);
    }

    @Override
    public void writeRecord(Record record) {
        writeRecords(Collections.singletonList(record));
    }

    @Override
    public void writeRecords(List<Record> records) {
        SortedMap<String, DataType> measurementMap = new TreeMap<>();
        for (Record record: records) {
            for (int index = 0; index < record.getLength(); index++) {
                String measurement = record.getMeasurement(index);
                DataType dataType = record.getDataType(index);
                if (measurementMap.getOrDefault(measurement, dataType) != dataType) {
                    throw new IllegalArgumentException("measurement " + measurement + " has multi data type, which is invalid.");
                }
                measurementMap.putIfAbsent(measurement, dataType);
            }
        }

        List<String> measurements = new ArrayList<>();
        List<DataType> dataTypeList = new ArrayList<>();
        Map<String, Integer> measurementIndexMap = new HashMap<>(); // measurement 对应的 index
        int index = 0;
        for (String measurement: measurementMap.keySet()) {
            measurementIndexMap.put(measurement, index);
            index++;
            measurements.add(measurement);
            dataTypeList.add(measurementMap.get(measurement));
        }


        SortedMap<Long, Object[]> valuesMap = new TreeMap<>();
        for (Record record: records) {
            long timestamp = record.getTimestamp();
            Object[] values = valuesMap.getOrDefault(timestamp, new Object[measurements.size()]);
            for (int i = 0; i < record.getValues().size(); i++) {
                String measurement = record.getMeasurement(i);
                int measurementIndex = measurementIndexMap.get(measurement);
                values[measurementIndex] = record.getValue(i);
            }
            valuesMap.put(timestamp, values);
        }

        long[] timestamps = new long[valuesMap.size()];
        Object[][] valuesList = new Object[valuesMap.size()][];
        index = 0;
        for (Map.Entry<Long, Object[]> entry: valuesMap.entrySet()) {
            timestamps[index] = entry.getKey();
            valuesList[index] = entry.getValue();
            index++;
        }
        writeRowData(measurements, timestamps, valuesList, dataTypeList);
    }

    @Override
    public <M> void writeMeasurement(M measurement) {
        writeMeasurements(Collections.singletonList(measurement));
    }

    @Override
    public <M> void writeMeasurements(List<M> measurements) {
        writeRecords(measurements.stream().map(measurementMapper::toRecord).collect(Collectors.toList()));
    }

    @Override
    public void writeTable(Table table) {
        long[] timestamps = new long[table.getLength()];
        Object[][] valuesList = new Object[table.getLength()][];
        for (int i = 0; i < table.getLength(); i++) {
            timestamps[i] = table.getTimestamp(i);
            valuesList[i] = table.getValues(i);
        }
        writeRowData(table.getMeasurements(), timestamps, valuesList, table.getDataTypes());
    }

    private void writeColumnData(List<String> paths, long[] timestamps, Object[][] valuesList,
                                 List<DataType> dataTypeList) {
        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (int i = 0; i < valuesList.length; i++) {
            Object[] values = valuesList[i];
            valueBufferList.add(ByteUtils.getColumnByteBuffer(values, dataTypeList.get(i)));
            Bitmap bitmap = new Bitmap(values.length);
            for (int j = 0; j < values.length; j++) {
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

    private void writeRowData(List<String> paths, long[] timestamps, Object[][] valuesList,
                              List<DataType> dataTypeList) {
        List<ByteBuffer> valueBufferList = new ArrayList<>();
        List<ByteBuffer> bitmapBufferList = new ArrayList<>();
        for (Object[] values : valuesList) {
            valueBufferList.add(ByteUtils.getRowByteBuffer(values, dataTypeList));
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
        req.setDataTypeList(dataTypeList);
        req.setAttributesList(new ArrayList<>());

        try {
            lock.lock();
            Status status = client.insertNonAlignedRowRecords(req);
            RpcUtils.verifySuccess(status);
        } catch (TException | ExecutionException e) {
            logger.error("insert data failure: ", e);
        } finally {
            lock.unlock();
        }

    }

}
