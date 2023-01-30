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
package cn.edu.tsinghua.iginx.influxdb.query.entity;

import static cn.edu.tsinghua.iginx.influxdb.tools.TimeUtils.instantToNs;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.influxdb.tools.SchemaTransformer;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InfluxDBHistoryQueryRowStream implements RowStream {

    private final List<Map.Entry<String, List<FluxTable>>> bucketQueryResults;

    private final List<int[]> indexList;

    private final Header header;

    private int hasMoreRecords;

    private int size;

    public InfluxDBHistoryQueryRowStream(Map<String, List<FluxTable>> bucketQueryResults, List<String> patterns) {
        this.bucketQueryResults = new ArrayList<>(bucketQueryResults.entrySet());
        this.indexList = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < bucketQueryResults.size(); i++) {
            String bucket = this.bucketQueryResults.get(i).getKey();
            List<FluxTable> tables = this.bucketQueryResults.get(i).getValue();
            this.indexList.add(new int[tables.size()]);
            for (FluxTable table: tables) {
                fields.add(SchemaTransformer.toField(bucket, table));
                this.hasMoreRecords++;
                this.size++;
            }
        }

        header = new Header(Field.KEY, fields);
    }

    @Override
    public Header getHeader() throws PhysicalException {
        return header;
    }

    @Override
    public void close() throws PhysicalException {
        // need to do nothing
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        return this.hasMoreRecords != 0;
    }

    @Override
    public Row next() throws PhysicalException {
        long timestamp = Long.MAX_VALUE;
        for (int i = 0; i < this.bucketQueryResults.size(); i++) {
            int[] indices = indexList.get(i);
            List<FluxTable> tables = bucketQueryResults.get(i).getValue();
            for (int j = 0; j < tables.size(); j++) {
                int index = indices[j];
                FluxTable table = tables.get(j);
                List<FluxRecord> records = table.getRecords();
                if (index == records.size()) { // 数据已经消费完毕了
                    continue;
                }
                FluxRecord record = records.get(index);
                timestamp = Math.min(instantToNs(record.getTime()), timestamp);
            }
        }
        if (timestamp == Long.MAX_VALUE) {
            return null;
        }
        Object[] values = new Object[size];
        int ptr = 0;
        for (int i = 0; i < this.bucketQueryResults.size(); i++) {
            int[] indices = indexList.get(i);
            List<FluxTable> tables = bucketQueryResults.get(i).getValue();
            for (int j = 0; j < tables.size(); j++) {
                int index = indices[j];
                FluxTable table = tables.get(j);
                List<FluxRecord> records = table.getRecords();
                if (index == records.size()) { // 数据已经消费完毕了
                    values[ptr++] = null;
                    continue;
                }
                FluxRecord record = records.get(index);
                if (instantToNs(record.getTime()) == timestamp) {
                    DataType dataType = header.getField(ptr).getType();
                    Object value = record.getValue();
                    if (dataType == DataType.BINARY) {
                        value = ((String) value).getBytes();
                    }
                    values[ptr++] = value;
                    indices[j]++;
                    if (indices[j] == records.size()) {
                        hasMoreRecords--;
                    }
                } else {
                    values[ptr++] = null;
                }
            }
        }
        return new Row(header, timestamp, values);
    }
}
