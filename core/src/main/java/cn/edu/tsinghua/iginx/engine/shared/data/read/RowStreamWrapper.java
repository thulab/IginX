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
package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;

public class RowStreamWrapper implements RowStream {

    private final RowStream rowStream;

    private Row nextRow; // 如果不为空，表示 row stream 的下一行已经取出来，并缓存在 wrapper 里

    public RowStreamWrapper(RowStream rowStream) {
        this.rowStream = rowStream;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        return rowStream.getHeader();
    }

    @Override
    public void close() throws PhysicalException {
        rowStream.close();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        return nextRow != null || rowStream.hasNext();
    }

    @Override
    public Row next() throws PhysicalException {
        Row row = null;
        if (nextRow != null) { // 本地已经缓存了下一行
            row = nextRow;
            nextRow = null;
        } else {
            row = rowStream.next();
        }
        return row;
    }

    public long nextTimestamp() throws PhysicalException {
        if (nextRow == null) { // 本地已经缓存了下一行
            nextRow = rowStream.next();
        }
        return nextRow.getKey();
    }

}
