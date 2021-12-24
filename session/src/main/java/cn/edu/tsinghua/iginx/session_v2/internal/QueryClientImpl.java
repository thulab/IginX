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
import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.AggregateQuery;
import cn.edu.tsinghua.iginx.session_v2.query.DownsampleQuery;
import cn.edu.tsinghua.iginx.session_v2.query.IginXColumn;
import cn.edu.tsinghua.iginx.session_v2.query.IginXHeader;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.LastQuery;
import cn.edu.tsinghua.iginx.session_v2.query.Query;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.http.concurrent.Cancellable;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

public class QueryClientImpl extends AbstractFunctionClient implements QueryClient {

    private static final Logger logger = LoggerFactory.getLogger(QueryClientImpl.class);

    private final ResultMapper resultMapper;

    public QueryClientImpl(IginXClientImpl iginXClient, ResultMapper resultMapper) {
        super(iginXClient);
        this.resultMapper = resultMapper;
    }

    private IginXTable simpleQuery(SimpleQuery query) {
        List<String> measurements = new ArrayList<>(query.getMeasurements());
        QueryDataReq req = new QueryDataReq(sessionId, MeasurementUtils.mergeAndSortMeasurements(measurements), query.getStartTime(), query.getEndTime());

        QueryDataResp resp;

        synchronized (iginXClient) {
            iginXClient.checkIsClosed();
            try {
                resp = client.queryData(req);
                RpcUtils.verifySuccess(resp.status);
            } catch (TException | ExecutionException e) {
                throw new IginXException("simple query failure: ", e);
            }
        }

        return buildIginXTable(resp.getQueryDataSet(), resp.getPaths(), resp.getDataTypeList());
    }

    private IginXTable aggregateQuery(AggregateQuery query) throws IginXException {
        List<String> measurements = new ArrayList<>(query.getMeasurements());
        AggregateQueryReq req = new AggregateQueryReq(sessionId, MeasurementUtils.mergeAndSortMeasurements(measurements), query.getStartTime(), query.getEndTime(), query.getAggregateType());

        AggregateQueryResp resp;

        synchronized (iginXClient) {
            iginXClient.checkIsClosed();
            try {
                resp = client.aggregateQuery(req);
                RpcUtils.verifySuccess(resp.status);
            } catch (TException | ExecutionException e) {
                throw new IginXException("aggregate query failure: ", e);
            }
        }

        return null;
    }

    private IginXTable downsampleQuery(DownsampleQuery query) throws IginXException {
        List<String> measurements = new ArrayList<>(query.getMeasurements());
        DownsampleQueryReq req = new DownsampleQueryReq(sessionId, MeasurementUtils.mergeAndSortMeasurements(measurements), query.getStartTime(), query.getEndTime(), query.getAggregateType(), query.getPrecision());

        DownsampleQueryResp resp;
        synchronized (iginXClient) {
            iginXClient.checkIsClosed();
            try {
                resp = client.downsampleQuery(req);
                RpcUtils.verifySuccess(resp.status);
            } catch (TException | ExecutionException e) {
                throw new IginXException("downsample query failure: ", e);
            }
        }
        return buildIginXTable(resp.getQueryDataSet(), resp.getPaths(), resp.getDataTypeList());
    }

    private IginXTable lastQuery(LastQuery query) throws IginXException {
        return null;
    }

    @Override
    public <M> List<M> query(Query query, Class<M> measurementType) throws IginXException {
        IginXTable table = query(query);
        return table.getRecords().stream().map(e -> resultMapper.toPOJO(e, measurementType)).filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public IginXTable query(Query query) throws IginXException {
        return null;
    }

    @Override
    public void query(Query query, BiConsumer<Cancellable, IginXRecord> onNext) throws IginXException {

    }

    @Override
    public <M> void query(Query query, Class<M> measurementType, BiConsumer<Cancellable, M> onNext) throws IginXException {

    }


    @Override
    public IginXTable query(String query) throws IginXException {
        return null;
    }

    @Override
    public <M> void query(String query, Class<M> measurementType, BiConsumer<Cancellable, M> onNext) throws IginXException {

    }

    @Override
    public void query(String query, BiConsumer<Cancellable, IginXRecord> onNext) throws IginXException {

    }

    private IginXTable buildIginXTable(QueryDataSet dataSet, List<String> measurements, List<DataType> dataTypes) {
        boolean hasTimestamp = dataSet.getTimestamps() == null;
        long[] timestamps = new long[0];
        if (hasTimestamp) {
            timestamps = getLongArrayFromByteBuffer(dataSet.timestamps);
        }

        List<IginXColumn> columns = new ArrayList<>();
        Map<Integer, String> columnIndexMap = new HashMap<>();
        for (int i = 0; i < measurements.size(); i++) {
            String measurement = measurements.get(i);
            DataType dataType = dataTypes.get(i);
            columns.add(new IginXColumn(measurement, dataType));
            columnIndexMap.put(i, measurement);
        }
        IginXHeader header = new IginXHeader(hasTimestamp ? IginXColumn.TIME : null, columns);
        List<IginXRecord> records = new ArrayList<>();
        for (int i = 0; i < dataSet.valuesList.size(); i++) {
            ByteBuffer valuesBuffer = dataSet.valuesList.get(i);
            ByteBuffer bitmapBuffer = dataSet.bitmapList.get(i);
            Bitmap bitmap = new Bitmap(dataTypes.size(), bitmapBuffer.array());
            Map<String, Object> values = new HashMap<>();
            for (int j = 0; j < dataTypes.size(); j++) {
                if (bitmap.get(j)) {
                    values.put(columnIndexMap.get(j), getValueFromByteBufferByDataType(valuesBuffer, dataTypes.get(j)));
                }
            }
            records.add(new IginXRecord(hasTimestamp ? timestamps[i] : 0L, header, values));
        }
        return new IginXTable(header, records);
    }

}
