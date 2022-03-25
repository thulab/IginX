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

import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPhysicalTask implements PhysicalTask {

  private static final Logger logger = LoggerFactory.getLogger(AbstractPhysicalTask.class);

  private final TaskType type;

  private final List<Operator> operators;
  private final CountDownLatch resultLatch = new CountDownLatch(1);
  private PhysicalTask followerTask;
  private TaskExecuteResult result;

  public AbstractPhysicalTask(TaskType type, List<Operator> operators) {
    this.type = type;
    this.operators = operators;
  }

  @Override
  public TaskType getType() {
    return type;
  }

  @Override
  public List<Operator> getOperators() {
    return operators;
  }

  @Override
  public PhysicalTask getFollowerTask() {
    return followerTask;
  }

  @Override
  public void setFollowerTask(PhysicalTask task) {
    this.followerTask = task;
  }

  @Override
  public TaskExecuteResult getResult() {
    try {
      this.resultLatch.await();
    } catch (InterruptedException e) {
      logger.error("unexpected interrupted when get result: ", e);
    }
    return result;
  }

  @Override
  public void setResult(TaskExecuteResult result) {
    this.result = result;
    this.resultLatch.countDown();
  }
}
