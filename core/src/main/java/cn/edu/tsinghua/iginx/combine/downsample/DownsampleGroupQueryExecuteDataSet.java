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
package cn.edu.tsinghua.iginx.combine.downsample;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.entity.RowRecord;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.ByteUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DownsampleGroupQueryExecuteDataSet implements QueryExecuteDataSet {

    private final List<String> columnNames;

    private final List<DataType> columnTypes;

    private final long[] timestamps;

    private final List<Object> values;

    private long currentTimestamp;

    public DownsampleGroupQueryExecuteDataSet(long timestamp, AggregateQueryResp resp) {
        this.columnNames = resp.getPaths();
        this.columnTypes = resp.dataTypeList;
        if (resp.getTimestamps() == null) {
            this.timestamps = new long[this.columnNames.size()];
            Arrays.fill(this.timestamps, timestamp);
        } else {
            this.timestamps = ByteUtils.getLongArrayFromByteArray(resp.getTimestamps());
            for (int i = 0; i < timestamps.length; i++) {
                if (timestamps[i] == -1) {
                    timestamps[i] = timestamp;
                }
            }
        }
        this.values = Arrays.asList(ByteUtils.getValuesByDataType(resp.valuesList, this.columnTypes));
        this.currentTimestamp = -1;
    }

    @Override
    public List<String> getColumnNames() throws ExecutionException {
        List<String> columnNames = new ArrayList<>(Collections.singletonList("Time"));
        columnNames.addAll(this.columnNames);
        return columnNames;
    }

    @Override
    public List<DataType> getColumnTypes() throws ExecutionException {
        List<DataType> columnTypes = new ArrayList<>(Collections.singletonList(DataType.LONG));
        columnTypes.addAll(this.columnTypes);
        return columnTypes;
    }

    @Override
    public boolean hasNext() throws ExecutionException {
        long timestamp = currentTimestamp;
        for (long l : timestamps) {
            if (l > currentTimestamp) {
                if (timestamp != currentTimestamp) {
                    timestamp = Math.min(timestamp, l);
                } else {
                    timestamp = l;
                }
            }
        }
        if (timestamp == currentTimestamp) {
            return false;
        }
        currentTimestamp = timestamp;
        return true;
    }

    @Override
    public RowRecord next() throws ExecutionException {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < timestamps.length; i++) {
            if (currentTimestamp == timestamps[i]) {
                values.add(this.values.get(i));
            } else {
                values.add(null);
            }
        }
        return new RowRecord(currentTimestamp, values);
    }

    @Override
    public void close() throws ExecutionException {
        // DO NOTHING
    }
}
