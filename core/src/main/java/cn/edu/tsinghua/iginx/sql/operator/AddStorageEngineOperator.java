package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.thrift.AddStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;

import java.util.ArrayList;
import java.util.List;

public class AddStorageEngineOperator extends Operator {

    private List<StorageEngine> engines;

    public AddStorageEngineOperator() {
        engines = new ArrayList<>();
        this.operatorType = OperatorType.ADD_STORAGE_ENGINE;
    }

    public List<StorageEngine> getEngines() {
        return engines;
    }

    public void setEngines(StorageEngine engine) {
        this.engines.add(engine);
    }

    @Override
    public ExecuteSqlResp doOperation(long sessionId) {
        IginxWorker worker = IginxWorker.getInstance();
        AddStorageEnginesReq req = new AddStorageEnginesReq(sessionId, engines);
        return new ExecuteSqlResp(worker.addStorageEngines(req), SqlType.AddStorageEngines);
    }
}
