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
import cn.edu.tsinghua.iginx.engine.physical.optimizer.ReplicaDispatcher;
import cn.edu.tsinghua.iginx.engine.physical.optimizer.rule.Rule;
import cn.edu.tsinghua.iginx.engine.physical.task.BinaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.MultipleMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.PhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskType;
import cn.edu.tsinghua.iginx.engine.physical.task.UnaryMemoryPhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.constraint.ConstraintManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.MultipleOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.TimeFilter;
import cn.edu.tsinghua.iginx.engine.shared.source.OperatorSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class NaivePhysicalOptimizer implements PhysicalOptimizer {

    private static final Logger logger = LoggerFactory.getLogger(NaivePhysicalOptimizer.class);

    public static NaivePhysicalOptimizer getInstance() {
        return NaivePhysicalOptimizerHolder.INSTANCE;
    }

    @Override
    public PhysicalTask optimize(Operator root) {
        if (root == null) {
            return null;
        }
        return constructTask(root);
    }

    @Override
    public ConstraintManager getConstraintManager() {
        return NaiveConstraintManager.getInstance();
    }

    @Override
    public ReplicaDispatcher getReplicaDispatcher() {
        return NaiveReplicaDispatcher.getInstance();
    }

    @Override
    public void setRules(Collection<Rule> rules) {

    }

    private PhysicalTask constructTask(Operator operator) {
        if (OperatorType.isUnaryOperator(operator.getType())) {
            UnaryOperator unaryOperator = (UnaryOperator) operator;
            Source source = unaryOperator.getSource();
            if (source.getType() == SourceType.Fragment) { // 构建物理计划
                List<Operator> operators = new ArrayList<>();
                operators.add(operator);
                if (OperatorType.isNeedBroadcasting(operator.getType())) {
                    return new StoragePhysicalTask(operators, true, true);
                } else {
                    return new StoragePhysicalTask(operators);
                }
            } else { // 构建内存中的计划
                OperatorSource operatorSource = (OperatorSource) source;
                Operator sourceOperator = operatorSource.getOperator();
                PhysicalTask sourceTask = constructTask(operatorSource.getOperator());
                if (sourceTask instanceof StoragePhysicalTask && sourceOperator.getType() == OperatorType.Project && ((UnaryOperator) sourceOperator).getSource().getType() == SourceType.Fragment
                    && operator.getType() == OperatorType.Select) {
                    Select select = (Select) operator;
                    if (select.getFilter() instanceof AndFilter) {
                        AndFilter andFilter = (AndFilter) select.getFilter();
                        List<Filter> filters = andFilter.getChildren();
                        if (filters.size() == 2 && filters.get(0) instanceof TimeFilter && filters.get(1) instanceof TimeFilter) {
                            logger.info("[TPCx-IoT] push down time filter");
                            sourceTask.getOperators().add(operator);
                            return sourceTask;
                        }
                    }
                }
                PhysicalTask task = new UnaryMemoryPhysicalTask(Arrays.asList(operator), sourceTask);
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
            for (Source source : sources) {
                OperatorSource operatorSource = (OperatorSource) source;
                PhysicalTask parentTask = constructTask(operatorSource.getOperator());
                parentTasks.add(parentTask);
            }
            PhysicalTask task = new MultipleMemoryPhysicalTask(Collections.singletonList(operator), parentTasks);
            for (PhysicalTask parentTask : parentTasks) {
                parentTask.setFollowerTask(task);
            }
            return task;
        }
    }

    private static class NaivePhysicalOptimizerHolder {

        private static final NaivePhysicalOptimizer INSTANCE = new NaivePhysicalOptimizer();

        private NaivePhysicalOptimizerHolder() {
        }

    }

}
