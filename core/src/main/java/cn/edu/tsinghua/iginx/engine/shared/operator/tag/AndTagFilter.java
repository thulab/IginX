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
package cn.edu.tsinghua.iginx.engine.shared.operator.tag;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AndTagFilter implements TagFilter {

    private final List<TagFilter> children;

    public AndTagFilter(List<TagFilter> children) {
        this.children = children;
    }

    public List<TagFilter> getChildren() {
        return children;
    }

    @Override
    public TagFilterType getType() {
        return TagFilterType.And;
    }

    @Override
    public TagFilter copy() {
        List<TagFilter> newChildren = new ArrayList<>();
        children.forEach(e -> newChildren.add(e.copy()));
        return new AndTagFilter(newChildren);
    }

    @Override
    public String toString() {
        return children.stream().map(Object::toString).collect(Collectors.joining(" && ", "(", ")"));
    }
}
