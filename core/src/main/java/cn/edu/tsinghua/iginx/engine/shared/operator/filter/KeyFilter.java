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
package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import java.util.Objects;

public class KeyFilter implements Filter {

    private final FilterType type = FilterType.Key;

    private final long value;
    private Op op;

    public KeyFilter(Op op, long value) {
        this.op = op;
        this.value = value;
    }

    public void reverseFunc() {
        this.op = Op.getOpposite(op);
    }

    public Op getOp() {
        return op;
    }

    public long getValue() {
        return value;
    }

    @Override
    public void accept(FilterVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public FilterType getType() {
        return type;
    }

    @Override
    public Filter copy() {
        return new KeyFilter(op, value);
    }

    @Override
    public String toString() {
        return "key " + Op.op2Str(op) + " " + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyFilter that = (KeyFilter) o;
        return value == that.value && type == that.type && op == that.op;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value, op);
    }
}
