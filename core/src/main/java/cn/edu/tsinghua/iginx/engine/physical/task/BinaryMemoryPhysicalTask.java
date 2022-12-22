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
package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BinaryMemoryPhysicalTask extends MemoryPhysicalTask {

    private static final Logger logger = LoggerFactory.getLogger(BinaryMemoryPhysicalTask.class);

    private final PhysicalTask parentTaskA;

    private final PhysicalTask parentTaskB;

    public BinaryMemoryPhysicalTask(List<Operator> operators, PhysicalTask parentTaskA, PhysicalTask parentTaskB) {
        super(TaskType.BinaryMemory, operators);
        this.parentTaskA = parentTaskA;
        this.parentTaskB = parentTaskB;
    }

    public PhysicalTask getParentTaskA() {
        return parentTaskA;
    }

    public PhysicalTask getParentTaskB() {
        return parentTaskB;
    }

    @Override
    public TaskExecuteResult execute() {
        TaskExecuteResult parentResultA = parentTaskA.getResult();
        if (parentResultA == null) {
            return new TaskExecuteResult(new PhysicalException("unexpected parent task execute result for " + this + ": null"));
        }
        if (parentResultA.getException() != null) {
            return parentResultA;
        }
        TaskExecuteResult parentResultB = parentTaskB.getResult();
        if (parentResultB == null) {
            return new TaskExecuteResult(new PhysicalException("unexpected parent task execute result for " + this + ": null"));
        }
        if (parentResultB.getException() != null) {
            return parentResultB;
        }
        List<Operator> operators = getOperators();
        RowStream streamA = parentResultA.getRowStream();
        RowStream streamB = parentResultB.getRowStream();
        RowStream stream;
        OperatorMemoryExecutor executor = OperatorMemoryExecutorFactory.getInstance().getMemoryExecutor();
        try {
            Operator op = operators.get(0);
            if (OperatorType.isUnaryOperator(op.getType())) {
                throw new UnexpectedOperatorException("unexpected unary operator " + op + " in unary task");
            }
            stream = executor.executeBinaryOperator((BinaryOperator) op, streamA, streamB);
            for (int i = 1; i < operators.size(); i++) {
                op = operators.get(i);
                if (OperatorType.isBinaryOperator(op.getType())) {
                    throw new UnexpectedOperatorException("unexpected binary operator " + op + " in unary task");
                }
                stream = executor.executeUnaryOperator((UnaryOperator) op, stream);
            }
        } catch (PhysicalException e) {
            logger.error("encounter error when execute operator in memory: ", e);
            return new TaskExecuteResult(e);
        }
        return new TaskExecuteResult(stream);
    }

    @Override
    public boolean notifyParentReady() {
        return parentReadyCount.incrementAndGet() == 2;
    }
}
