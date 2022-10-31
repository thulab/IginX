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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

import java.util.Collections;
import java.util.List;

public class Table implements RowStream {

    public static final Table EMPTY_TABLE = new Table(Header.EMPTY_HEADER, Collections.emptyList());

    private final Header header;

    private final List<Row> rows;

    private int index;

    public Table(Header header, List<Row> rows) {
        this.header = header;
        this.rows = rows;
        this.index = 0;
    }

    @Override
    public Header getHeader() {
        return header;
    }

    @Override
    public boolean hasNext() {
        return index < rows.size();
    }

    public boolean isEmpty() {
        return rows == null || rows.isEmpty();
    }

    @Override
    public Row next() {
        Row row = rows.get(index);
        index++;
        return row;
    }

    public List<Row> getRows() {
        return rows;
    }

    public Row getRow(int index) {
        return rows.get(index);
    }

    public int getRowSize() {
        return rows.size();
    }

    @Override
    public void close() {

    }

    public void reset() {
        this.index = 0;
    }

}
