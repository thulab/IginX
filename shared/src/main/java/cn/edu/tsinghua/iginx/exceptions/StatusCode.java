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

public enum StatusCode {
    WRONG_USERNAME_OR_PASSWORD(100),
    ACCESS_DENY(101),

    SUCCESS_STATUS(200),
    PARTIAL_SUCCESS(204),

    REDIRECT(300),

    SESSION_ERROR(400),
    STATEMENT_EXECUTION_ERROR(401),
    STATEMENT_PARSE_ERROR(402),

    SYSTEM_ERROR(500),
    SERVICE_UNAVAILABLE(503),

    JOB_FINISHED(600),
    JOB_CREATED(601),
    JOB_RUNNING(602),
    JOB_FAILING(603),
    JOB_FAILED(604),
    JOB_CLOSING(605),
    JOB_CLOSED(606);

    private int statusCode;

    StatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
