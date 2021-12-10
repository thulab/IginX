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

import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.query.AggregateQuery;
import cn.edu.tsinghua.iginx.session_v2.query.IginXColumn;
import cn.edu.tsinghua.iginx.session_v2.query.IginXHeader;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.Query;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.apache.http.concurrent.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

public class QueryClientImpl extends AbstractFunctionClient implements QueryClient {

    private static final Logger logger = LoggerFactory.getLogger(QueryClientImpl.class);

    public QueryClientImpl(IginXClientImpl iginXClient) {
        super(iginXClient);
    }

    @Override
    public IginXTable simpleQuery(SimpleQuery query) {
        List<String> measurements = new ArrayList<>(query.getMeasurements());
        QueryDataReq req = new QueryDataReq(sessionId, mergeAndSortMeasurements(measurements), query.getStartTime(), query.getEndTime());

        try {
            lock.lock();
            QueryDataResp resp = client.queryData(req);
            RpcUtils.verifySuccess(resp.status);
            return buildIginXTable(resp.getQueryDataSet(), resp.getPaths(), resp.getDataTypeList());
        } catch (Exception e) {
            logger.error("encounter error when execute query data: ", e);
            return IginXTable.EMPTY_TABLE;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public IginXTable aggregateQuery(AggregateQuery query) {
        return null;
    }

    @Override
    public <M> List<M> query(Query query, Class<M> measurementType) {
        return null;
    }

    @Override
    public IginXTable query(String query) {
        return null;
    }

    @Override
    public IginXTable query(Query query) {
        return query(query.getQuery());
    }

    @Override
    public void query(String query, BiConsumer<Cancellable, IginXRecord> onNext) {

    }

    @Override
    public void query(Query query, BiConsumer<Cancellable, IginXRecord> onNext) {
        query(query.getQuery(), onNext);
    }

    @Override
    public <M> void query(String query, Class<M> measurementType, BiConsumer<Cancellable, M> onNext) {

    }

    @Override
    public <M> void query(Query query, Class<M> measurementType, BiConsumer<Cancellable, M> onNext) {

    }

    private List<String> mergeAndSortMeasurements(List<String> measurements) {
        if (measurements.stream().anyMatch(x -> x.equals("*"))) {
            List<String> tempPaths = new ArrayList<>();
            tempPaths.add("*");
            return tempPaths;
        }
        List<String> prefixes = measurements.stream().filter(x -> x.contains("*")).map(x -> x.substring(0, x.indexOf("*"))).collect(Collectors.toList());
        if (prefixes.isEmpty()) {
            Collections.sort(measurements);
            return measurements;
        }
        List<String> mergedMeasurements = new ArrayList<>();
        for (String measurement : measurements) {
            if (!measurement.contains("*")) {
                boolean skip = false;
                for (String prefix : prefixes) {
                    if (measurement.startsWith(prefix)) {
                        skip = true;
                        break;
                    }
                }
                if (skip) {
                    continue;
                }
            }
            mergedMeasurements.add(measurement);
        }
        mergedMeasurements.sort(String::compareTo);
        return mergedMeasurements;
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
