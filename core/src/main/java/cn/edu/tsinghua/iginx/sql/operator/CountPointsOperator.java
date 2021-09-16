package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.combine.AggregateCombineResult;
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.AggregateQueryContext;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.RpcUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CountPointsOperator extends Operator {

    public CountPointsOperator() {
        this.operatorType = OperatorType.COUNT_POINTS;
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        List<String> paths = new ArrayList<>(Arrays.asList("*"));
        Core core = Core.getInstance();
        AggregateQueryReq req = new AggregateQueryReq(
                sessionId,
                paths,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                AggregateType.COUNT
        );
        AggregateQueryContext ctx = new AggregateQueryContext(req);
        core.processRequest(ctx);
        AggregateQueryResp aggregateQueryResp = ((AggregateCombineResult) ctx.getCombineResult()).getResp();
        SessionAggregateQueryDataSet dataSet = new SessionAggregateQueryDataSet(aggregateQueryResp, AggregateType.COUNT);

        long count = 0;
        for (Object value : dataSet.getValues()) {
            count += (long) value;
        }

        ExecuteSqlResp resp = new ExecuteSqlResp(RpcUtils.SUCCESS, SqlType.CountPoints);
        resp.setPointsNum(count);
        return resp;
    }
}
