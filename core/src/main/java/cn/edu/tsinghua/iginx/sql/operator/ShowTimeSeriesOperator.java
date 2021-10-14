package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsReq;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;

public class ShowTimeSeriesOperator extends Operator {

    public ShowTimeSeriesOperator() {
        this.operatorType = OperatorType.SHOW_TIME_SERIES;
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        IginxWorker worker = IginxWorker.getInstance();
        ShowColumnsReq req = new ShowColumnsReq(sessionId);

        ShowColumnsResp showColumnsResp = worker.showColumns(req);
        ExecuteSqlResp resp = new ExecuteSqlResp(showColumnsResp.getStatus(), SqlType.ShowTimeSeries);

        resp.setPaths(showColumnsResp.getPaths());
        resp.setDataTypeList(showColumnsResp.getDataTypeList());
        return resp;
    }
}
