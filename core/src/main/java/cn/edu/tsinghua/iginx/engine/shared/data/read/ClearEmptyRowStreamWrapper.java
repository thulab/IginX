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

public class ClearEmptyRowStreamWrapper implements RowStream {

    private final RowStream stream;

    private Row nextRow;

    public ClearEmptyRowStreamWrapper(RowStream stream) {
        this.stream = stream;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        return stream.getHeader();
    }

    @Override
    public void close() throws PhysicalException {
        stream.close();
    }

    @Override
    public boolean hasNext() throws PhysicalException { // 调用 hasNext 之后，如果返回 true，那么 nextRow 必然存在
        if (nextRow != null) {
            return true;
        }
        loadNextRow();
        return nextRow != null;
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new PhysicalException("the row stream has used up");
        }
        Row row = nextRow;
        nextRow = null;
        return row;
    }

    private void loadNextRow() throws PhysicalException {
        if (nextRow != null) {
            return;
        }
        do {
            if (!stream.hasNext()) {
                nextRow = null;
                break;
            }
            nextRow = stream.next();
        } while (nextRow.isEmpty());

    }
}
