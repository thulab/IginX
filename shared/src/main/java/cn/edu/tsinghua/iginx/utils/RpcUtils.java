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
package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcUtils {

    private static final Logger logger = LoggerFactory.getLogger(RpcUtils.class);

    public static Status WRONG_USERNAME_OR_PASSWORD = new Status(StatusCode.WRONG_USERNAME_OR_PASSWORD.getStatusCode());

    public static Status ACCESS_DENY = new Status(StatusCode.ACCESS_DENY.getStatusCode());

    public static Status SUCCESS = new Status(StatusCode.SUCCESS_STATUS.getStatusCode());

    public static Status PARTIAL_SUCCESS = new Status(StatusCode.PARTIAL_SUCCESS.getStatusCode());

    public static Status FAILURE = new Status(StatusCode.STATEMENT_EXECUTION_ERROR.getStatusCode());

    public static Status JOB_FINISHED = new Status(StatusCode.JOB_FINISHED.getStatusCode());
    public static Status JOB_CREATED = new Status(StatusCode.JOB_CREATED.getStatusCode());
    public static Status JOB_RUNNING = new Status(StatusCode.JOB_RUNNING.getStatusCode());
    public static Status JOB_FAILING = new Status(StatusCode.JOB_FAILING.getStatusCode());
    public static Status JOB_FAILED = new Status(StatusCode.JOB_FAILED.getStatusCode());
    public static Status JOB_CLOSING = new Status(StatusCode.JOB_CLOSING.getStatusCode());
    public static Status JOB_CLOSED = new Status(StatusCode.JOB_CLOSED.getStatusCode());

    static {
        WRONG_USERNAME_OR_PASSWORD.setMessage("wrong username or password");
        ACCESS_DENY.setMessage("access deny");
        PARTIAL_SUCCESS.setMessage("partial success");
        FAILURE.setMessage("unexpected error");
    }

    public static void verifySuccess(Status status) throws ExecutionException {
        if (status.code != StatusCode.SUCCESS_STATUS.getStatusCode()) {
            throw new ExecutionException(status);
        }
    }

    public static Status status(StatusCode code, String msg) {
        Status status = new Status(code.getStatusCode());
        status.setMessage(msg);
        return status;
    }

    public static boolean verifyNoRedirect(Status status) {
        return status.code != StatusCode.REDIRECT.getStatusCode();
    }
}
