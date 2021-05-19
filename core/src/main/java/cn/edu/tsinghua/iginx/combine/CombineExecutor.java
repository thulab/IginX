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

import cn.edu.tsinghua.iginx.combine.aggregate.AggregateCombiner;
import cn.edu.tsinghua.iginx.combine.downsample.DownsampleCombiner;
import cn.edu.tsinghua.iginx.combine.querydata.QueryDataSetCombiner;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.query.result.*;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class CombineExecutor implements ICombineExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CombineExecutor.class);

    private final AggregateCombiner aggregateCombiner = AggregateCombiner.getInstance();

    private final QueryDataSetCombiner queryDataSetCombiner = QueryDataSetCombiner.getInstance();


    @Override
    public CombineResult combineResult(RequestContext requestContext) {
        CombineResult combineResult;
        List<PlanExecuteResult> planExecuteResults = requestContext.getPlanExecuteResults();
        Status status = RpcUtils.SUCCESS;
        int failureCount = (int) planExecuteResults.stream().filter(e -> e.getStatusCode() == PlanExecuteResult.FAILURE).count();
        if (failureCount > 0)
            status = RpcUtils.PARTIAL_SUCCESS;
        if (failureCount == planExecuteResults.size() && failureCount != 0)
            status = RpcUtils.FAILURE;

        switch (requestContext.getType()) {
            case QueryData:
                QueryDataResp queryDataResp = new QueryDataResp();
                queryDataResp.setStatus(status);
                try {
                    queryDataSetCombiner.combineResult(queryDataResp, planExecuteResults.stream().map(QueryDataPlanExecuteResult.class::cast).collect(Collectors.toList()));
                } catch (ExecutionException e) {
                    logger.error("encounter error when combine query data results: ", e);
                }
                combineResult = new QueryDataCombineResult(status, queryDataResp);
                break;
            case AggregateQuery:
                AggregateQueryResp aggregateQueryResp = new AggregateQueryResp();
                aggregateQueryResp.setStatus(status);
                AggregateQueryReq req = ((AggregateQueryContext) requestContext).getReq();
                switch (req.aggregateType) {
                    case COUNT:
                    case SUM:
                        aggregateCombiner.combineSumOrCountResult(aggregateQueryResp, planExecuteResults.stream()
                                .map(StatisticsAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case MAX:
                        aggregateCombiner.combineMaxResult(aggregateQueryResp, planExecuteResults.stream()
                                .map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case MIN:
                        aggregateCombiner.combineMinResult(aggregateQueryResp, planExecuteResults.stream()
                                .map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case FIRST:
                        aggregateCombiner.combineFirstResult(aggregateQueryResp, planExecuteResults.stream()
                                .map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case LAST:
                        aggregateCombiner.combineLastResult(aggregateQueryResp, planExecuteResults.stream()
                                .map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case AVG:
                        aggregateCombiner.combineAvgResult(aggregateQueryResp, planExecuteResults.stream()
                                .map(AvgAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                }
                combineResult = new AggregateCombineResult(status, aggregateQueryResp);
                break;
            case DownsampleQuery:
                DownsampleQueryResp downsampleQueryResp = new DownsampleQueryResp();
                downsampleQueryResp.setStatus(status);
                DownsampleQueryReq downsampleQueryReq = ((DownsampleQueryContext) requestContext).getReq();
                try {
                    DownsampleCombiner.combineDownsampleQueryResult(downsampleQueryResp, planExecuteResults, downsampleQueryReq.aggregateType);
                } catch (Exception e) {
                    logger.error("encounter error when combine downsample data results: ", e);
                }
                combineResult = new DownsampleQueryCombineResult(status, downsampleQueryResp);
                break;

            default:
                combineResult = new NonDataCombineResult(status);
        }
        return combineResult;
    }
}
