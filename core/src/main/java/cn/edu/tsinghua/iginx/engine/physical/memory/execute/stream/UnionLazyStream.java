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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Union;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UnionLazyStream extends BinaryLazyStream {

    private final Union union;

    private boolean hasInitialized = false;

    private Header header;

    private Row nextA;

    private Row nextB;

    public UnionLazyStream(Union union, RowStream streamA, RowStream streamB) {
        super(streamA, streamB);
        this.union = union;
    }

    private void initialize() throws PhysicalException {
        if (hasInitialized) {
            return;
        }
        Header headerA = streamA.getHeader();
        Header headerB = streamB.getHeader();
        if (headerA.hasKey() ^ headerB.hasKey()) {
            throw new InvalidOperatorParameterException("row stream to be union must have same fields");
        }
        boolean hasTimestamp = headerA.hasKey();
        Set<Field> targetFieldSet = new HashSet<>();
        targetFieldSet.addAll(headerA.getFields());
        targetFieldSet.addAll(headerB.getFields());
        List<Field> targetFields = new ArrayList<>(targetFieldSet);
        if (hasTimestamp) {
            header = new Header(Field.KEY, targetFields);
        } else {
            header = new Header(targetFields);
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
        return nextA != null || nextB != null || streamA.hasNext() || streamB.hasNext();
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        if (!header.hasKey()) {
            // 不包含时间戳，只需要迭代式的顺次访问两个 stream 即可
            if (streamA.hasNext()) {
                return streamA.next();
            }
            return streamB.next();
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
            return RowUtils.transform(row, header);
        }
        if (nextB == null) { // 流 B 被消费完毕
            Row row = nextA;
            nextA = null;
            return RowUtils.transform(row, header);
        }
        if (nextA.getKey() <= nextB.getKey()) {
            Row row = nextA;
            nextA = null;
            return RowUtils.transform(row, header);
        } else {
            Row row = nextB;
            nextB = null;
            return RowUtils.transform(row, header);
        }
    }
}
