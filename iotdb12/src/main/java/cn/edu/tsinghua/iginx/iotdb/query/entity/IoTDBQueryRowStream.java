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
package cn.edu.tsinghua.iginx.iotdb.query.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

public class IoTDBQueryRowStream implements RowStream {

    private static final String PREFIX = "root.";

    private final SessionDataSetWrapper dataset;

    private final Header header;

    public IoTDBQueryRowStream(SessionDataSetWrapper dataset) {
        this.dataset = dataset;

        List<String> names = dataset.getColumnNames();
        List<String> types = dataset.getColumnTypes();

        Field time = null;
        List<Field> fields = new ArrayList<>();

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String type = types.get(i);
            if (i == 0 && name.equals("Time")) {
                time = Field.TIME;
                continue;
            }
            fields.add(new Field(transformColumnName(name), DataTypeTransformer.strFromIoTDB(type)));
        }

        if (time == null) {
            this.header = new Header(fields);
        } else {
            this.header = new Header(time, fields);
        }
    }

    private String transformColumnName(String columnName) {
        if (columnName.indexOf('(') != -1) {
            columnName = columnName.substring(columnName.indexOf('(') + 1, columnName.length() - 1);
        }
        if (columnName.startsWith(PREFIX)) {
            columnName = columnName.substring(columnName.indexOf('.', columnName.indexOf('.') + 1) + 1);
        }
        return columnName;
    }

    @Override
    public Header getHeader() {
        return this.header;
    }

    @Override
    public void close() {
        dataset.close();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        try {
            return dataset.hasNext();
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RowFetchException(e);
        }
    }

    @Override
    public Row next() throws PhysicalException {
        try {
            RowRecord record = dataset.next();
            long timestamp = record.getTimestamp();
            Object[] fields = new Object[record.getFields().size()];
            for (int i = 0; i < fields.length; i++) {
                org.apache.iotdb.tsfile.read.common.Field field = record.getFields().get(i);
                if (field.getDataType() == TEXT) {
                    fields[i] = field.getBinaryV().getValues();
                } else {
                    fields[i] = field.getObjectValue(field.getDataType());
                }
            }
            return new Row(header, timestamp, fields);
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RowFetchException(e);
        }
    }
}
