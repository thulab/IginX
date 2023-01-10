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
package cn.edu.tsinghua.iginx.postgresql.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.NotFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import java.util.stream.Collectors;

public class FilterTransformer {

    public static final long MAX_TIMESTAMP = Integer.MAX_VALUE;

    public static String toString(Filter filter) {
        if (filter == null) {
            return "";
        }
        switch (filter.getType()) {
            case And:
                return toString((AndFilter) filter);
            case Or:
                return toString((OrFilter) filter);
            case Not:
                return toString((NotFilter) filter);
            case Value:
                return toString((ValueFilter) filter);
            case Key:
                return toString((KeyFilter) filter);
            default:
                return "";
        }
    }

    private static String toString(AndFilter filter) {
        return filter.getChildren().stream().map(FilterTransformer::toString).collect(Collectors.joining(" and ", "(", ")"));
    }

    private static String toString(NotFilter filter) {
        return "not " + filter.toString();
    }

    private static String toString(KeyFilter filter) {
        return "time " + Op.op2Str(filter.getOp()) + " to_timestamp(" + Math.min(filter.getValue(), MAX_TIMESTAMP) + ")";
    }

    private static String toString(ValueFilter filter) {
        return filter.getPath() + " " + Op.op2Str(filter.getOp()) + " " + filter.getValue().getValue();
    }

    private static String toString(OrFilter filter) {
        return filter.getChildren().stream().map(FilterTransformer::toString).collect(Collectors.joining(" or ", "(", ")"));
    }



}
