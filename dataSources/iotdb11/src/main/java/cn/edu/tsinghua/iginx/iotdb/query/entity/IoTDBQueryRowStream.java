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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

public class IoTDBQueryRowStream implements RowStream {

    enum State {
        HAS_NEXT,
        NO_NEXT,
        UNKNOWN,
    }

    private static final String PREFIX = "root.";

    private static final String UNIT = "unit";

    private boolean[] filterMap;

    private final SessionDataSetWrapper dataset;

    private final boolean trimStorageUnit;

    private final Header header;

    private State state;

    public IoTDBQueryRowStream(SessionDataSetWrapper dataset, boolean trimStorageUnit) {
        this.dataset = dataset;
        this.trimStorageUnit = trimStorageUnit;

        List<String> names = dataset.getColumnNames();
        List<TSDataType> types = dataset.getColumnTypes();

        Field time = null;
        List<Field> fields = new ArrayList<>();

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            TSDataType type = types.get(i);
            if (i == 0 && name.equals("Time")) {
                time = Field.TIME;
                continue;
            }
            Field field = new Field(transformColumnName(name), DataTypeTransformer.fromIoTDB(type));
            if (!this.trimStorageUnit && field.getName().startsWith(UNIT)) {
                continue;
            }
            fields.add(field);
        }

        if (!this.trimStorageUnit) {
            if (time == null) {
                this.filterMap = new boolean[names.size()];
                for (int i = 0; i < names.size(); i++) {
                    filterMap[i] = names.get(i).startsWith(PREFIX + UNIT);
                }
            } else {
                this.filterMap = new boolean[names.size() - 1];
                for (int i = 1; i < names.size(); i++) {
                    filterMap[i - 1] = names.get(i).startsWith(PREFIX + UNIT);
                }
            }
        }

        if (time == null) {
            this.header = new Header(fields);
        } else {
            this.header = new Header(time, fields);
        }

        this.state = State.UNKNOWN;
    }

    private String transformColumnName(String columnName) {
        if (columnName.indexOf('(') != -1) {
            columnName = columnName.substring(columnName.indexOf('(') + 1, columnName.length() - 1);
        }
        if (columnName.startsWith(PREFIX)) {
            columnName = columnName.substring(columnName.indexOf('.', trimStorageUnit ? columnName.indexOf('.') + 1: 0) + 1);
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
            if (state == State.UNKNOWN) {
                if (dataset.hasNext()) {
                    state = State.HAS_NEXT;
                } else {
                    state = State.NO_NEXT;
                }
            }
            return state == State.HAS_NEXT;
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RowFetchException(e);
        }
    }

    @Override
    public Row next() throws PhysicalException {
        try {
            RowRecord record = dataset.next();
            long timestamp = record.getTimestamp();
            Object[] fields = new Object[header.getFieldSize()];
            int index = 0;
            for (int i = 0; i < fields.length; i++) {
                if (!trimStorageUnit && filterMap[i]) {
                    continue;
                }
                org.apache.iotdb.tsfile.read.common.Field field = record.getFields().get(i);
                if (field.getDataType() == TEXT) {
                    fields[index++] = field.getBinaryV().getValues();
                } else {
                    fields[index++] = field.getObjectValue(field.getDataType());
                }
            }
            state = State.UNKNOWN;
            return new Row(header, timestamp, fields);
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RowFetchException(e);
        }
    }
}
