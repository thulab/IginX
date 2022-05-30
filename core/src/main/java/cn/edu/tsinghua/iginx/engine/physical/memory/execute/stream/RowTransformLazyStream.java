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

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;

import java.util.Map;

public class RowTransformLazyStream extends UnaryLazyStream {

    private final RowTransform rowTransform;

    private final RowMappingFunction function;

    private final Map<String, Value> params;

    private Row nextRow;

    private Header header;

    public RowTransformLazyStream(RowTransform rowTransform, RowStream stream) {
        super(stream);
        this.rowTransform = rowTransform;
        this.function = (RowMappingFunction) rowTransform.getFunctionCall().getFunction();
        this.params = rowTransform.getFunctionCall().getParams();
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (header == null) {
            if (nextRow == null) {
                nextRow = calculateNext();
            }
            header = nextRow == null ? Header.EMPTY_HEADER : nextRow.getHeader();
        }
        return header;
    }

    private Row calculateNext() throws PhysicalException {
        try {
            while(stream.hasNext()) {
                Row row = function.transform(stream.next(), params);
                if (row != null) {
                    return row;
                }
            }
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException("encounter error when execute row mapping function " + function.getIdentifier() + ".", e);
        }
        return null;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (nextRow == null) {
            nextRow = calculateNext();
        }
        return nextRow != null;
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        Row row = nextRow;
        nextRow = null;
        return row;
    }
}
