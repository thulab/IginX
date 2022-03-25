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
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

public class TaskExecuteResult {

  private RowStream rowStream;

  private PhysicalException exception;

  public TaskExecuteResult() {

  }

  public TaskExecuteResult(RowStream rowStream) {
    this(rowStream, null);
  }

  public TaskExecuteResult(PhysicalException exception) {
    this(null, exception);
  }

  public TaskExecuteResult(RowStream rowStream, PhysicalException exception) {
    this.rowStream = rowStream;
    this.exception = exception;
  }

  public RowStream getRowStream() {
    return rowStream;
  }

  public void setRowStream(RowStream rowStream) {
    this.rowStream = rowStream;
  }

  public PhysicalException getException() {
    return exception;
  }

  public void setException(PhysicalException exception) {
    this.exception = exception;
  }
}
