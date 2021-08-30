package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.combine.ShowColumnsCombineResult;
import cn.edu.tsinghua.iginx.core.Core;
import cn.edu.tsinghua.iginx.core.context.ShowColumnsContext;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;

public class ShowTimeSeriesOperator extends Operator {

    public ShowTimeSeriesOperator(){
        this.operatorType = OperatorType.SHOW_TIME_SERIES;
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        Core core = Core.getInstance();
        ShowColumnsReq req = new ShowColumnsReq(sessionId);
        ShowColumnsContext ctx = new ShowColumnsContext(req);
        core.processRequest(ctx);

        ExecuteSqlResp resp = new ExecuteSqlResp(ctx.getStatus(), SqlType.ShowTimeSeries);
        ShowColumnsResp showColumnsResp = ((ShowColumnsCombineResult) ctx.getCombineResult()).getResp();
        resp.setPaths(showColumnsResp.getPaths());
        resp.setDataTypeList(showColumnsResp.getDataTypeList());
        return resp;
    }
}
