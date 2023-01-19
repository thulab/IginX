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
package cn.edu.tsinghua.iginx.engine.shared.operator.type;

public enum OperatorType {

    // Exception[0,9]
    Unknown(0),

    // MultipleOperator[10,19]
    CombineNonQuery(10),

    //isGlobalOperator[20,29]
    ShowTimeSeries(20),
    Migration,

    // BinaryOperator[30,39]
    Join(30),
    Union,
    InnerJoin,
    OuterJoin,
    CrossJoin,


    // isUnaryOperator >= 40
    Binary(40),
    Unary,
    Delete,
    Insert,
    Multiple,
    Project,
    Select,
    Sort,
    Limit,
    Downsample,
    RowTransform,
    SetTransform,
    MappingTransform,
    Rename,
    Reorder,
    AddSchemaPrefix;



    private int value;
    OperatorType(){
        this(OperatorTypeCounter.nextValue);
    }
    OperatorType(int value){
        this.value = value;
        OperatorTypeCounter.nextValue = value + 1;
    }

    public int getValue()
    {
        return value;
    }

    private static class OperatorTypeCounter
    {
        private static int nextValue = 0;
    }

    public static boolean isBinaryOperator(OperatorType op) {
        return op.value >= 30 && op.value <= 39;
    }

    public static boolean isUnaryOperator(OperatorType op) {
        return op.value >= 40;
    }

    public static boolean isMultipleOperator(OperatorType op) {
        return op == CombineNonQuery;
    }

    public static boolean isGlobalOperator(OperatorType op) {
        return op == ShowTimeSeries || op == Migration;
    }

    public static boolean isNeedBroadcasting(OperatorType op) {
        return op == Delete || op == Insert;
    }

}
