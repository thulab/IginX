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
package cn.edu.tsinghua.iginx.exceptions;

import cn.edu.tsinghua.iginx.thrift.Status;

public class ExecutionException extends IginxException {

  private static final long serialVersionUID = -7769482614133326007L;

  public ExecutionException(Status status) {
    super(status.message, status.code);
  }

  public ExecutionException(String message) {
    super(message, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }

  public ExecutionException(Throwable cause) {
    super(cause, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }

  public ExecutionException(String message, Throwable cause) {
    super(message, cause, StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());
  }
}
