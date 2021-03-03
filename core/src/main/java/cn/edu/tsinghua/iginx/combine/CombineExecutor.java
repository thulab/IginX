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
package cn.edu.tsinghua.iginx.combine;

import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

import java.util.List;
import java.util.stream.Collectors;

public class CombineExecutor implements ICombineExecutor {

    private final QueryDataSetCombiner queryDataSetCombiner = QueryDataSetCombiner.getInstance();

    protected QueryDataCombineResult combineQueryDataResult(List<QueryDataPlanExecuteResult> executeResults, Status status) {
        return null;
    }

    @Override
    public CombineResult combineResult(RequestContext requestContext) {
        CombineResult combineResult;
        List<PlanExecuteResult> planExecuteResults = requestContext.getPlanExecuteResults();
        Status status = RpcUtils.SUCCESS;
        int failureCount = (int) planExecuteResults.stream().filter(e -> e.getStatusCode() == PlanExecuteResult.FAILURE).count();
        if (failureCount > 0)
            status = RpcUtils.PARTIAL_SUCCESS;
        if (failureCount == planExecuteResults.size())
            status = RpcUtils.FAILURE;

        switch (requestContext.getType()) {
            case QueryData:
                combineResult = combineQueryDataResult(planExecuteResults.stream().map(QueryDataPlanExecuteResult.class::cast).collect(Collectors.toList()), status);
                break;
            default:
                combineResult = new NonDataCombineResult(status);
        }
        return combineResult;
    }
}
