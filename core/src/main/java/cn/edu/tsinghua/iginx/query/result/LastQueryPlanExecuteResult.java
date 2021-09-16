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

import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.thrift.DataType;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class LastQueryPlanExecuteResult extends SyncPlanExecuteResult {

    private List<String> paths;

    private List<DataType> dataTypes;

    private List<Long> times;

    private List<Object> values;

    public LastQueryPlanExecuteResult(int statusCode, IginxPlan plan) {
        super(statusCode, plan);
        this.paths = new ArrayList<>();
        this.dataTypes = new ArrayList<>();
        this.times = new ArrayList<>();
        this.values = new ArrayList<>();
    }

}
