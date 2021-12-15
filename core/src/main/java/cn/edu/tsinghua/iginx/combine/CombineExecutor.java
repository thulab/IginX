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
import cn.edu.tsinghua.iginx.combine.meta.ShowColumnsCombiner;
import cn.edu.tsinghua.iginx.combine.querydata.QueryDataSetCombiner;
import cn.edu.tsinghua.iginx.combine.valuefilter.ValueFilterCombiner;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.core.context.DownsampleQueryContext;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.core.context.ValueFilterQueryContext;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.StatusCode;
import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.LastQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ShowColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.ValueFilterQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryReq;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryResp;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class CombineExecutor implements ICombineExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CombineExecutor.class);

    private final AggregateCombiner aggregateCombiner = AggregateCombiner.getInstance();

    private final QueryDataSetCombiner queryDataSetCombiner = QueryDataSetCombiner.getInstance();

    private final ValueFilterCombiner valueFilterCombiner = ValueFilterCombiner.getInstance();

    @Override
    public CombineResult combineResult(RequestContext requestContext) {
        CombineResult combineResult;
        List<PlanExecuteResult> planExecuteResults = requestContext.getPlanExecuteResults();
        StatusCode statusCode = StatusCode.SUCCESS_STATUS;
        String statusMessage = null;

        int failureCount = (int) planExecuteResults.stream().filter(e -> e.getStatusCode() == PlanExecuteResult.FAILURE).count();
        if (failureCount > 0) {
            StringBuilder errorMessageBuilder = new StringBuilder();
            if (failureCount == planExecuteResults.size()) {
                statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
                errorMessageBuilder.append("Request execute failure.");
            } else {
                statusCode = StatusCode.PARTIAL_SUCCESS;
                errorMessageBuilder.append("Request execute partial failure.");
            }
            statusMessage = errorMessageBuilder.toString();
        }

        switch (requestContext.getType()) {
            case QueryData:
                QueryDataResp queryDataResp = new QueryDataResp();
                queryDataResp.setStatus(RpcUtils.SUCCESS);
                try {
                    queryDataSetCombiner.combineResult(queryDataResp, planExecuteResults.stream().map(QueryDataPlanExecuteResult.class::cast).collect(Collectors.toList()));
                } catch (ExecutionException e) {
                    logger.error("encounter error when combine query data results: ", e);
                    statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
                    statusMessage = "Combine execute results failed: " + e.getMessage();
                }
                combineResult = new QueryDataCombineResult(RpcUtils.status(statusCode, statusMessage), queryDataResp);
                break;
            case AggregateQuery:
                AggregateQueryResp aggregateQueryResp = new AggregateQueryResp();
                aggregateQueryResp.setStatus(RpcUtils.SUCCESS);
                AggregateQueryReq aggregateQueryReq = ((AggregateQueryContext) requestContext).getReq();
                List<Integer> aggregateGroupByLevels = aggregateQueryReq.getGroupByLevels();
                boolean needGroup = aggregateGroupByLevels != null && aggregateGroupByLevels.size() != 0;
                switch (aggregateQueryReq.aggregateType) {
                    case COUNT:
                    case SUM:
                        if (needGroup) {
                            aggregateCombiner.combineSumOrCountResult(aggregateQueryResp, planExecuteResults.stream()
                                    .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).map(StatisticsAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()), aggregateGroupByLevels);
                        } else {
                            aggregateCombiner.combineSumOrCountResult(aggregateQueryResp, planExecuteResults.stream()
                                    .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).map(StatisticsAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        }
                        break;
                    case MAX:
                        aggregateCombiner.combineMaxResult(aggregateQueryResp, planExecuteResults.stream()
                                .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case MIN:
                        aggregateCombiner.combineMinResult(aggregateQueryResp, planExecuteResults.stream()
                                .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case FIRST_VALUE:
                        aggregateCombiner.combineFirstResult(aggregateQueryResp, planExecuteResults.stream()
                                .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case LAST_VALUE:
                        aggregateCombiner.combineLastResult(aggregateQueryResp, planExecuteResults.stream()
                                .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).map(SingleValueAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        break;
                    case AVG:
                        if (needGroup) {
                            aggregateCombiner.combineAvgResult(aggregateQueryResp, planExecuteResults.stream()
                                    .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).map(AvgAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()), aggregateGroupByLevels);
                        } else {
                            aggregateCombiner.combineAvgResult(aggregateQueryResp, planExecuteResults.stream()
                                    .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).map(AvgAggregateQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                        }

                }
                combineResult = new AggregateCombineResult(RpcUtils.status(statusCode, statusMessage), aggregateQueryResp);
                break;
            case DownsampleQuery:
                DownsampleQueryResp downsampleQueryResp = new DownsampleQueryResp();
                downsampleQueryResp.setStatus(RpcUtils.SUCCESS);
                DownsampleQueryReq downsampleQueryReq = ((DownsampleQueryContext) requestContext).getReq();
                List<Integer> downsampleGroupByLevels = downsampleQueryReq.getGroupByLevels();
                if (downsampleGroupByLevels != null) {
                    downsampleGroupByLevels.sort(Integer::compareTo);
                }
                try {
                    DownsampleCombiner.combineDownsampleQueryResult(downsampleQueryResp, planExecuteResults.stream().filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode()).collect(Collectors.toList()),
                            downsampleQueryReq.aggregateType, downsampleGroupByLevels);
                } catch (Exception e) {
                    logger.error("encounter error when combine downsample data results: ", e);
                    statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
                    statusMessage = "Combine execute results failed: " + e.getMessage();
                }
                combineResult = new DownsampleQueryCombineResult(RpcUtils.status(statusCode, statusMessage), downsampleQueryResp);
                break;
            case ValueFilterQuery:
                ValueFilterQueryResp valueFilterQueryResp = new ValueFilterQueryResp();
                valueFilterQueryResp.setStatus(RpcUtils.SUCCESS);
                try {
                    valueFilterCombiner.combineResult(valueFilterQueryResp, planExecuteResults.stream().map(ValueFilterQueryPlanExecuteResult.class::cast).collect(Collectors.toList()),
                            ((ValueFilterQueryContext) requestContext).getReq().getPaths(), ((ValueFilterQueryContext) requestContext).getBooleanExpression());
                } catch (ExecutionException e) {
                    logger.error("encounter error when combine query data results: ", e);
                    statusCode = StatusCode.STATEMENT_EXECUTION_ERROR;
                    statusMessage = "Combine execute results failed: " + e.getMessage();
                }
                combineResult = new ValueFilterCombineResult(RpcUtils.status(statusCode, statusMessage), valueFilterQueryResp);
                break;
            case LastQuery:
                LastQueryResp lastQueryResp = new LastQueryResp();
                lastQueryResp.setStatus(RpcUtils.SUCCESS);
                LastQueryCombiner.getInstance().combineResult(lastQueryResp, planExecuteResults.stream()
                        .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode())
                        .map(LastQueryPlanExecuteResult.class::cast).collect(Collectors.toList()));
                combineResult = new LastQueryCombineResult(RpcUtils.status(statusCode, statusMessage), lastQueryResp);
                break;
            case ShowColumns:
                ShowColumnsResp showColumnsResp = new ShowColumnsResp();
                showColumnsResp.setStatus(RpcUtils.SUCCESS);
                ShowColumnsCombiner.getInstance().combineResult(showColumnsResp, planExecuteResults.stream()
                        .filter(e -> e.getStatusCode() == StatusCode.SUCCESS_STATUS.getStatusCode())
                        .map(ShowColumnsPlanExecuteResult.class::cast).collect(Collectors.toList()));
                combineResult = new ShowColumnsCombineResult(RpcUtils.status(statusCode, statusMessage), showColumnsResp);
                break;
            default:
                combineResult = new NonDataCombineResult(RpcUtils.status(statusCode, statusMessage));
        }
        return combineResult;
    }
}
