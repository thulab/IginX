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
import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.DeleteClient;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DeleteDataInColumnsReq;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.thrift.TException;

import java.util.*;

public class DeleteClientImpl extends AbstractFunctionClient implements DeleteClient {

    private final MeasurementMapper measurementMapper;

    public DeleteClientImpl(IginXClientImpl iginXClient, MeasurementMapper measurementMapper) {
        super(iginXClient);
        this.measurementMapper = measurementMapper;
    }

    @Override
    public void deleteMeasurement(String measurement) throws IginXException {
        Arguments.checkNotNull(measurement, "measurement");
        deleteMeasurements(Collections.singletonList(measurement));
    }

    @Override
    public void deleteMeasurements(Collection<String> measurements) throws IginXException {
        Arguments.checkNotNull(measurements, "measurements");
        measurements.forEach(measurement -> Arguments.checkNotNull(measurement, "measurement"));

        DeleteColumnsReq req = new DeleteColumnsReq(sessionId, MeasurementUtils.mergeAndSortMeasurements(new ArrayList<>(measurements)));

        synchronized (iginXClient) {
            iginXClient.checkIsClosed();
            try {
                Status status = client.deleteColumns(req);
                RpcUtils.verifySuccess(status);
            } catch (TException | ExecutionException e) {
                throw new IginXException("delete measurements failure: ", e);
            }
        }
    }

    @Override
    public void deleteMeasurement(Class<?> measurementType) throws IginXException {
        Arguments.checkNotNull(measurementType, "measurementType");
        Collection<String> measurements = measurementMapper.toMeasurements(measurementType);
        deleteMeasurements(measurements);
    }

    @Override
    public void deleteMeasurementData(String measurement, long startTime, long endTime) throws IginXException {
        deleteMeasurementData(measurement, startTime, endTime, null, null);
    }

    @Override
    public void deleteMeasurementData(String measurement, long startTime, long endTime, TimePrecision timePrecision) throws IginXException {
        deleteMeasurementData(measurement, startTime, endTime, null, timePrecision);
    }

    @Override
    public void deleteMeasurementsData(Collection<String> measurements, long startTime, long endTime) throws IginXException {
        deleteMeasurementsData(measurements, startTime, endTime, null, null);
    }

    @Override
    public void deleteMeasurementsData(Collection<String> measurements, long startTime, long endTime, TimePrecision timePrecision) throws IginXException {
        deleteMeasurementsData(measurements, startTime, endTime, null, timePrecision);
    }

    @Override
    public void deleteMeasurementData(Class<?> measurementType, long startTime, long endTime) throws IginXException {
        deleteMeasurementData(measurementType, startTime, endTime, null, null);
    }

    @Override
    public void deleteMeasurementData(Class<?> measurementType, long startTime, long endTime, TimePrecision timePrecision) throws IginXException {
        deleteMeasurementData(measurementType, startTime, endTime, null, timePrecision);
    }

    @Override
    public void deleteMeasurementData(String measurement, long startTime, long endTime, Map<String, List<String>> tagsList) throws IginXException {
        Arguments.checkNotNull(measurement, "measurement");
        deleteMeasurementsData(Collections.singletonList(measurement), startTime, endTime, tagsList, null);
    }

    @Override
    public void deleteMeasurementData(String measurement, long startTime, long endTime, Map<String, List<String>> tagsList, TimePrecision timePrecision) throws IginXException {
        Arguments.checkNotNull(measurement, "measurement");
        deleteMeasurementsData(Collections.singletonList(measurement), startTime, endTime, tagsList, timePrecision);
    }

    @Override
    public void deleteMeasurementsData(Collection<String> measurements, long startTime, long endTime, Map<String, List<String>> tagsList) throws IginXException {
        deleteMeasurementsData(measurements, startTime, endTime, tagsList, null);
    }

    @Override
    public void deleteMeasurementsData(Collection<String> measurements, long startTime, long endTime, Map<String, List<String>> tagsList, TimePrecision timePrecision) throws IginXException {
        Arguments.checkNotNull(measurements, "measurements");
        measurements.forEach(measurement -> Arguments.checkNotNull(measurement, "measurement"));

        DeleteDataInColumnsReq req = new DeleteDataInColumnsReq(sessionId, MeasurementUtils.mergeAndSortMeasurements(new ArrayList<>(measurements)), startTime, endTime);

        if (tagsList != null && !tagsList.isEmpty()) {
            req.setTagsList(tagsList);
        }
        req.setTimePrecision(timePrecision);

        synchronized (iginXClient) {
            iginXClient.checkIsClosed();
            try {
                Status status = client.deleteDataInColumns(req);
                RpcUtils.verifySuccess(status);
            } catch (TException | ExecutionException e) {
                throw new IginXException("delete measurements data failure: ", e);
            }
        }
    }

    @Override
    public void deleteMeasurementData(Class<?> measurementType, long startTime, long endTime, Map<String, List<String>> tagsList) throws IginXException {
        Arguments.checkNotNull(measurementType, "measurementType");
        Collection<String> measurements = measurementMapper.toMeasurements(measurementType);
        deleteMeasurementsData(measurements, startTime, endTime, tagsList);
    }

    @Override
    public void deleteMeasurementData(Class<?> measurementType, long startTime, long endTime, Map<String, List<String>> tagsList, TimePrecision timePrecision) throws IginXException {
        Arguments.checkNotNull(measurementType, "measurementType");
        Collection<String> measurements = measurementMapper.toMeasurements(measurementType);
        deleteMeasurementsData(measurements, startTime, endTime, tagsList, timePrecision);
    }
}
