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
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;

import java.util.ArrayDeque;
import java.util.Deque;

public class SortLazyStream extends UnaryLazyStream {

    private final Sort sort;

    private boolean asc;

    private Deque<Row> stack;

    public SortLazyStream(Sort sort, RowStream stream) {
        super(stream);
        this.sort = sort;
        this.asc = sort.getSortType() == Sort.SortType.ASC;
        if (!asc) {
            stack = new ArrayDeque<>();
        }
    }

    @Override
    public Header getHeader() throws PhysicalException {
        return stream.getHeader();
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (asc) {
            return stream.hasNext();
        }
        return !stack.isEmpty() || stream.hasNext();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        if (asc) {
            return stream.next();
        }
        while(stream.hasNext()) {
            stack.push(stream.next());
        }
        return stack.pop();
    }
}
