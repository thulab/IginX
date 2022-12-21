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
package cn.edu.tsinghua.iginx.engine.physical.optimizer.naive;

import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

import java.util.List;

class NaiveConstraintManager implements ConstraintManager {

    private static final NaiveConstraintManager INSTANCE = new NaiveConstraintManager();

    private NaiveConstraintManager() {
    }

    public static NaiveConstraintManager getInstance() {
        return INSTANCE;
    }

    private boolean checkOperator(Operator operator) {
        if (OperatorType.isBinaryOperator(operator.getType())) {
            return checkBinaryOperator((BinaryOperator) operator);
        }
        if (OperatorType.isUnaryOperator(operator.getType())) {
            return checkUnaryOperator((UnaryOperator) operator);
        }
        if (OperatorType.isMultipleOperator(operator.getType())) {
            return checkMultipleOperator((MultipleOperator) operator);
        }
        return OperatorType.isGlobalOperator(operator.getType()); // 未能识别的操作符
    }

    @Override
    public boolean check(Operator root) {
        if (root == null) {
            return false;
        }
        return checkOperator(root);
    }

    private boolean checkBinaryOperator(BinaryOperator binaryOperator) {
        Source sourceA = binaryOperator.getSourceA();
        Source sourceB = binaryOperator.getSourceB();
        if (sourceA == null || sourceB == null) {
            return false;
        }
        if (sourceA.getType() == SourceType.Fragment || sourceB.getType() == SourceType.Fragment) { // binary 的操作符的来源应该均为别的操作符的输出
            return false;
        }
        Operator sourceOperatorA = ((OperatorSource) sourceA).getOperator();
        Operator sourceOperatorB = ((OperatorSource) sourceB).getOperator();
        return checkOperator(sourceOperatorA) && checkOperator(sourceOperatorB);
    }

    private boolean checkUnaryOperator(UnaryOperator unaryOperator) {
        Source source = unaryOperator.getSource();
        if (source == null) {
            return false;
        }
        if (source.getType() == SourceType.Fragment) {
            return unaryOperator.getType() == OperatorType.Project ||
                unaryOperator.getType() == OperatorType.Delete ||
                unaryOperator.getType() == OperatorType.Insert;
        }
        Operator sourceOperator = ((OperatorSource) source).getOperator();
        return checkOperator(sourceOperator);
    }

    public boolean checkMultipleOperator(MultipleOperator multipleOperator) {
        List<Source> sources = multipleOperator.getSources();
        for (Source source : sources) {
            if (source.getType() == SourceType.Fragment) {
                return false;
            }
            Operator sourceOperator = ((OperatorSource) source).getOperator();
            if (!checkOperator(sourceOperator)) {
                return false;
            }
        }
        return true;
    }
}
