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

public enum OperatorType {

    Unknown,
    Binary,
    Unary,
    Multiple,

    Project,
    Select,
    Join,
    Union,
    Sort,
    Limit,
    Downsample,
    RowTransform,
    SetTransform,
    MappingTransform,
    Rename,

    Delete,
    Insert,
    CombineNonQuery,

    ShowTimeSeries;

    public static boolean isBinaryOperator(OperatorType op) {
        return op == Join || op == Union;
    }

    public static boolean isUnaryOperator(OperatorType op) {
        return op == Project || op == Select || op == Sort || op == Limit || op == Downsample || op == RowTransform || op == SetTransform || op == MappingTransform || op == Delete || op == Insert || op == Rename;
    }

    public static boolean isMultipleOperator(OperatorType op) {
        return op == CombineNonQuery;
    }

    public static boolean isGlobalOperator(OperatorType op) {
        return op == ShowTimeSeries;
    }

    public static boolean isNeedBroadcasting(OperatorType op) {
        return op == Delete || op == Insert;
    }

}
