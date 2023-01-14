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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.influxdb.tools.DataTypeTransformer.fromInfluxDB;
import static cn.edu.tsinghua.iginx.influxdb.tools.TimeUtils.instantToNs;

public class InfluxDBQueryRowStream implements RowStream {

    private final Header header;

    private final List<FluxTable> tables;

    private final int[] indices;

    private int hasMoreRecords;

    public InfluxDBQueryRowStream(List<FluxTable> tables) {
        this.tables = tables.stream().filter(e -> e.getRecords().size() > 0).collect(Collectors.toList()); // 只保留还有数据的二维表

        List<Field> fields = new ArrayList<>();
        for (FluxTable table: this.tables) {
            String path;
            if (table.getRecords().get(0).getValueByKey("t") == null) {
                path = table.getRecords().get(0).getMeasurement() + "." + table.getRecords().get(0).getField();
            } else {
                path = table.getRecords().get(0).getMeasurement() + "." + table.getRecords().get(0).getValueByKey(InfluxDBSchema.TAG) + "." + table.getRecords().get(0).getField();
            }
            DataType dataType = fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType());
            fields.add(new Field(path, dataType));
        }
        this.header = new Header(Field.KEY, fields);
        this.indices = new int[this.tables.size()];

        this.hasMoreRecords = this.tables.size();
    }

    @Override
    public Header getHeader() {
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
        for (int i = 0; i < this.tables.size(); i++) {
            int index = indices[i];
            FluxTable table = this.tables.get(i);
            List<FluxRecord> records = table.getRecords();
            if (index == records.size()) { // 数据已经消费完毕了
                continue;
            }
            FluxRecord record = records.get(index);
            timestamp = Math.min(instantToNs(record.getTime()), timestamp);
        }
        if (timestamp == Long.MAX_VALUE) {
            return null;
        }
        Object[] values = new Object[this.tables.size()];
        for (int i = 0; i < this.tables.size(); i++) {
            int index = indices[i];
            FluxTable table = this.tables.get(i);
            List<FluxRecord> records = table.getRecords();
            if (index == records.size()) { // 数据已经消费完毕了
                continue;
            }
            FluxRecord record = records.get(index);
            if (instantToNs(record.getTime()) == timestamp) {
                DataType dataType = header.getField(i).getType();
                Object value = record.getValue();
                if (dataType == DataType.BINARY) {
                    value = ((String) value).getBytes();
                }
                values[i] = value;
                indices[i]++;
                if (indices[i] == records.size()) {
                    hasMoreRecords--;
                }
            }
        }
        return new Row(header, timestamp, values);
    }
}
