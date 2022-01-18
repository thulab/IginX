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

import cn.edu.tsinghua.iginx.engine.physical.optimizer.PhysicalOptimizer;
import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MultipleMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.CombineNonQuery;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class NaivePhysicalOptimizer implements PhysicalOptimizer {

    @Override
    public PhysicalTask optimize(Operator root) {
        if (root == null) {
            return null;
        }
        if (OperatorType.isGlobalOperator(root.getType())) {
            return constructGlobalTask(root);
        }
        return constructTask(root);
    }

    private PhysicalTask constructTask(Operator operator) {
        if (OperatorType.isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOperator = (UnaryOperator) operator;
            Source source = unaryOperator.getSource();
            if (source.getType() == SourceType.Fragment) { // 构建物理计划
                if (OperatorType.isNeedBroadcasting(operator.getType())) {
                    return new StoragePhysicalTask(Collections.singletonList(operator), true, true);
                } else {
                    return new StoragePhysicalTask(Collections.singletonList(operator));
                }
            } else { // 构建内存中的计划
                OperatorSource operatorSource = (OperatorSource) source;
                PhysicalTask sourceTask = constructTask(operatorSource.getOperator());
                PhysicalTask task = new UnaryMemoryPhysicalTask(Collections.singletonList(operator), sourceTask);
                sourceTask.setFollowerTask(task);
                return task;
            }
        } else if (OperatorType.isBinaryOperator(operator.getType())) {
            BinaryOperator binaryOperator = (BinaryOperator) operator;
            OperatorSource sourceA = (OperatorSource) binaryOperator.getSourceA();
            OperatorSource sourceB = (OperatorSource) binaryOperator.getSourceB();
            PhysicalTask sourceTaskA = constructTask(sourceA.getOperator());
            PhysicalTask sourceTaskB = constructTask(sourceB.getOperator());
            PhysicalTask task = new BinaryMemoryPhysicalTask(Collections.singletonList(operator), sourceTaskA, sourceTaskB);
            sourceTaskA.setFollowerTask(task);
            sourceTaskB.setFollowerTask(task);
            return task;
        } else {
            MultipleOperator multipleOperator = (MultipleOperator) operator;
            List<Source> sources = multipleOperator.getSources();
            List<PhysicalTask> parentTasks = new ArrayList<>();
            for (Source source: sources) {
                OperatorSource operatorSource = (OperatorSource) source;
                PhysicalTask parentTask = constructTask(operatorSource.getOperator());
                parentTasks.add(parentTask);
            }
            PhysicalTask task = new MultipleMemoryPhysicalTask(Collections.singletonList(operator), parentTasks);
            for (PhysicalTask parentTask: parentTasks) {
                parentTask.setFollowerTask(task);
            }
            return task;
        }
    }

    private PhysicalTask constructGlobalTask(Operator operator) {
        return null;
    }

    public static NaivePhysicalOptimizer getInstance() {
        return NaivePhysicalOptimizerHolder.INSTANCE;
    }

    private static class NaivePhysicalOptimizerHolder {

        private static final NaivePhysicalOptimizer INSTANCE = new NaivePhysicalOptimizer();

        private NaivePhysicalOptimizerHolder() {}

    }

}
