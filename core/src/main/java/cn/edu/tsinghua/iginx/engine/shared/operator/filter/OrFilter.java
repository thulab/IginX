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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class OrFilter implements Filter {

    private final FilterType type = FilterType.Or;

    private final List<Filter> children;

    public OrFilter(List<Filter> children) {
        this.children = children;
    }

    public List<Filter> getChildren() {
        return children;
    }

    @Override
    public void accept(FilterVisitor visitor) {
        visitor.visit(this);
        this.children.forEach(child -> child.accept(visitor));
    }

    @Override
    public FilterType getType() {
        return type;
    }

    @Override
    public Filter copy() {
        List<Filter> newChildren = new ArrayList<>();
        children.forEach(e -> newChildren.add(e.copy()));
        return new OrFilter(newChildren);
    }

    @Override
    public String toString() {
        return children.stream().map(Object::toString).collect(Collectors.joining(" || ", "(", ")"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OrFilter filter = (OrFilter) o;
        return type == filter.type && Objects.equals(children, filter.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, children);
    }
}
