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
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public abstract class BinaryLazyStream implements RowStream {

    protected final RowStream streamA;

    protected final RowStream streamB;

    public BinaryLazyStream(RowStream streamA, RowStream streamB) {
        this.streamA = streamA;
        this.streamB = streamB;
    }

    @Override
    public void close() throws PhysicalException {
        PhysicalException pe = null;
        try {
            streamA.close();
        } catch (PhysicalException e) {
            pe = e;
        }
        try {
            streamB.close();
        } catch (PhysicalException e) {
            pe = e;
        }
        if (pe != null) {
            throw pe;
        }
    }
}
