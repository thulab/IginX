package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
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

public class CountPointsStatement extends Statement {

    public CountPointsStatement() {
        this.statementType = StatementType.COUNT_POINTS;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) {
        List<String> paths = new ArrayList<>(Arrays.asList("*"));
        IginxWorker worker = IginxWorker.getInstance();
        AggregateQueryReq req = new AggregateQueryReq(
                sessionId,
                paths,
                Long.MIN_VALUE,
                Long.MAX_VALUE,
                AggregateType.COUNT
        );
        AggregateQueryResp aggregateQueryResp = worker.aggregateQuery(req);
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
