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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Join;

import java.util.ArrayList;
import java.util.List;

public class JoinLazyStream extends BinaryLazyStream {

    private final Join join;

    private boolean joinByTime = false;

    private boolean joinByOrdinal = false;

    private boolean hasInitialized = false;

    private Header header;

    private Row nextA;

    private Row nextB;

    public JoinLazyStream(Join join, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.join = join;
    }

    private void initialize() throws PhysicalException {
        if (hasInitialized) {
            return;
        }
        if (this.join.getJoinBy().equals(Constants.TIMESTAMP)) {
            joinByTime = true;
        }
        if (this.join.getJoinBy().equals(Constants.ORDINAL)) {
            joinByOrdinal = true;
        }
        if (!joinByTime && !joinByOrdinal) {
            throw new InvalidOperatorParameterException("join operator is not support for field " + join.getJoinBy() + " except for " + Constants.TIMESTAMP + " and " + Constants.ORDINAL);
        }
        Header headerA = streamA.getHeader();
        Header headerB = streamB.getHeader();
        if (joinByTime) {
            if (!headerA.hasTimestamp() || !headerB.hasTimestamp()) {
                throw new InvalidOperatorParameterException("row streams for join operator by time should have timestamp.");
            }
            // 检查 field
            for (Field field : headerA.getFields()) {
                if (headerB.indexOf(field) != -1) { // 二者的 field 存在交集
                    throw new PhysicalTaskExecuteFailureException("two source has shared field");
                }
            }
            List<Field> newFields = new ArrayList<>();
            newFields.addAll(headerA.getFields());
            newFields.addAll(headerB.getFields());
            header = new Header(Field.TIME, newFields);
        }
        if (joinByOrdinal) {
            if (headerA.hasTimestamp() || headerB.hasTimestamp()) {
                throw new InvalidOperatorParameterException("row streams for join operator by ordinal shouldn't have timestamp.");
            }
            for (Field field : headerA.getFields()) {
                if (headerB.indexOf(field) != -1) { // 二者的 field 存在交集
                    throw new PhysicalTaskExecuteFailureException("two source has shared field");
                }
            }
            List<Field> newFields = new ArrayList<>();
            newFields.addAll(headerA.getFields());
            newFields.addAll(headerB.getFields());
            header = new Header(newFields);
        }
        hasInitialized = true;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        return header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (!hasInitialized) {
            initialize();
        }
        return nextA != null && nextB != null || streamA.hasNext() || streamB.hasNext();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        if (nextA == null && streamA.hasNext()) {
            nextA = streamA.next();
        }
        if (nextB == null && streamB.hasNext()) {
            nextB = streamB.next();
        }
        if (nextA == null) { // 流 A 被消费完毕
            Row row = nextB;
            nextB = null;
            // 做一个转换
            return buildRow(null, row);
        }
        if (nextB == null) { // 流 B 被消费完毕
            Row row = nextA;
            nextA = null;
            return buildRow(row, null);
        }
        if (joinByOrdinal) {
            Row row = buildRow(nextA, nextB);
            nextA = null;
            nextB = null;
            return row;
        }
        if (joinByTime) {
            Row row;
            if (nextA.getTimestamp() == nextB.getTimestamp()) {
                row = buildRow(nextA, nextB);
                nextA = null;
                nextB = null;
            } else if (nextA.getTimestamp() < nextB.getTimestamp()) {
                row = buildRow(nextA, null);
                nextA = null;
            } else {
                row = buildRow(null, nextB);
                nextB = null;
            }
            return row;
        }
        return null;
    }

    private Row buildRow(Row rowA, Row rowB) {
        if (joinByTime) {
            long timestamp;
            Object[] values = new Object[header.getFieldSize()];
            if (rowA != null && rowB != null) {
                System.arraycopy(rowA.getValues(), 0, values, 0, rowA.getHeader().getFieldSize());
                System.arraycopy(rowB.getValues(), 0, values, rowA.getHeader().getFieldSize(), rowB.getHeader().getFieldSize());
                timestamp = rowA.getTimestamp();
            } else if (rowA != null) {
                System.arraycopy(rowA.getValues(), 0, values, 0, rowA.getHeader().getFieldSize());
                timestamp = rowA.getTimestamp();
            } else {
                System.arraycopy(rowB.getValues(), 0, values, header.getFieldSize() - rowB.getHeader().getFieldSize(), rowB.getHeader().getFieldSize());
                timestamp = rowB.getTimestamp();
            }
            return new Row(header, timestamp, values);
        }
        if (joinByOrdinal) {
            Object[] values = new Object[header.getFieldSize()];
            if (rowA != null && rowB != null) {
                System.arraycopy(rowA.getValues(), 0, values, 0, rowA.getHeader().getFieldSize());
                System.arraycopy(rowB.getValues(), 0, values, rowA.getHeader().getFieldSize(), rowB.getHeader().getFieldSize());
            } else if (rowA != null) {
                System.arraycopy(rowA.getValues(), 0, values, 0, rowA.getHeader().getFieldSize());
            } else {
                System.arraycopy(rowB.getValues(), 0, values, header.getFieldSize() - rowB.getHeader().getFieldSize(), rowB.getHeader().getFieldSize());
            }
            return new Row(header, values);
        }
        return null;
    }
}
