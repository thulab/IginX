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
package cn.edu.tsinghua.iginx.query.expression;

public class Operator {
    private OperatorType operatorType;
    private boolean reversed = false;

    Operator(OperatorType tp) {
        operatorType = tp;
    }

    public OperatorType getOperatorType() {
        return operatorType;
    }

    public void setOperatorType(OperatorType operatorType) {
        this.operatorType = operatorType;
    }

    public boolean isReversed() {
        return reversed;
    }

    public void setReversed(boolean reversed) {
        this.reversed = reversed;
    }

    void reverse() {
        if (operatorType == OperatorType.GT) operatorType = OperatorType.LTE;
        else if (operatorType == OperatorType.GTE) operatorType = OperatorType.LT;
        else if (operatorType == OperatorType.EQ) operatorType = OperatorType.NE;
        else if (operatorType == OperatorType.NE) operatorType = OperatorType.EQ;
        else if (operatorType == OperatorType.LTE) operatorType = OperatorType.GT;
        else if (operatorType == OperatorType.LT) operatorType = OperatorType.GTE;
        else if (operatorType == OperatorType.AND || operatorType == OperatorType.OR)
            reversed = true;
    }
}
