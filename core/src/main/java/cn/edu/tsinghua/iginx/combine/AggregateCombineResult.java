package cn.edu.tsinghua.iginx.combine;

import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.Status;

public class AggregateCombineResult extends DataCombineResult {

    private final AggregateQueryResp resp;

    public AggregateCombineResult(Status status, AggregateQueryResp resp) {
        super(status);
        this.resp = resp;
        this.resp.setStatus(status);
    }

    public AggregateQueryResp getResp() {
        return resp;
    }
}
