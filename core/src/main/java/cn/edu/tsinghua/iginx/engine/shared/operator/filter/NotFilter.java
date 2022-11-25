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

public class NotFilter implements Filter {

    private final FilterType type = FilterType.Not;

    private Filter child;

    public NotFilter(Filter child) {
        this.child = child;
    }

    public Filter getChild() {
        return child;
    }

    public void setChild(Filter child) {
        this.child = child;
    }

    @Override
    public void accept(FilterVisitor visitor) {
        visitor.visit(this);
        this.child.accept(visitor);
    }

    @Override
    public FilterType getType() {
        return type;
    }

    @Override
    public Filter copy() {
        return new NotFilter(child.copy());
    }

    @Override
    public String toString() {
        return "!" + child.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NotFilter notFilter = (NotFilter) o;
        return type == notFilter.type && Objects.equals(child, notFilter.child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, child);
    }
}
