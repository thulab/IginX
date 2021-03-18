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

import java.util.List;

public class AvgAggregateQueryPlanExecuteResult extends SyncPlanExecuteResult {

    private List<DataType> dataTypes;

    private List<Object> sums;

    private List<Object> counts;

    public AvgAggregateQueryPlanExecuteResult(int statusCode, IginxPlan plan) {
        super(statusCode, plan);
    }

    public List<DataType> getDataTypes() {
        return dataTypes;
    }

    public void setDataTypes(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public List<Object> getSums() {
        return sums;
    }

    public void setSums(List<Object> sums) {
        this.sums = sums;
    }

    public List<Object> getCounts() {
        return counts;
    }

    public void setCounts(List<Object> counts) {
        this.counts = counts;
    }
}
