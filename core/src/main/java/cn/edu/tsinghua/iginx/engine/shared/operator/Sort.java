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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Sort extends AbstractUnaryOperator {

    private final String sortBy;

    private final SortType sortType;

    public Sort(Source source) {
        this(source, Constants.KEY, SortType.ASC);
    }

    public Sort(Source source, String sortBy, SortType sortType) {
        super(OperatorType.Sort, source);
        if (sortBy == null) {
            throw new IllegalArgumentException("sortBy shouldn't be null");
        }
        if (sortType == null) {
            throw new IllegalArgumentException("sortType shouldn't be null");
        }
        this.sortBy = sortBy;
        this.sortType = sortType;
    }

    public String getSortBy() {
        return sortBy;
    }

    public SortType getSortType() {
        return sortType;
    }

    @Override
    public Operator copy() {
        return new Sort(getSource().copy(), sortBy, sortType);
    }

    public enum SortType {
        ASC,
        DESC
    }

}
