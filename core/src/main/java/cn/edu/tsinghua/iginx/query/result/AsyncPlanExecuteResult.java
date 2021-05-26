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
package cn.edu.tsinghua.iginx.query.result;

public class AsyncPlanExecuteResult extends PlanExecuteResult {

    private static AsyncPlanExecuteResult successInstance;

    private static AsyncPlanExecuteResult failureInstance;

    /**
     * 如果加入异步执行队列失败，则为 false，反之则为 true
     */
    private final boolean success;

    private AsyncPlanExecuteResult(boolean success) {
        super(200, null);
        this.success = success;
    }

    public static AsyncPlanExecuteResult getInstance(boolean success) {
        if (success) {
            if (successInstance == null) {
                synchronized (AsyncPlanExecuteResult.class) {
                    if (successInstance == null) {
                        successInstance = new AsyncPlanExecuteResult(true);
                    }
                }
            }
            return successInstance;
        } else {
            if (failureInstance == null) {
                synchronized (AsyncPlanExecuteResult.class) {
                    if (failureInstance == null) {
                        failureInstance = new AsyncPlanExecuteResult(false);
                    }
                }
            }
            return failureInstance;
        }
    }

    public boolean isSuccess() {
        return success;
    }
}
