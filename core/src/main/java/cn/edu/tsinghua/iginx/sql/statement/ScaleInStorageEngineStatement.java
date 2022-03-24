package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.thrift.AddStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.ScaleInStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import java.util.ArrayList;
import java.util.List;

public class ScaleInStorageEngineStatement extends SystemStatement {

    private final List<StorageEngine> engines;

    public ScaleInStorageEngineStatement() {
        engines = new ArrayList<>();
        this.statementType = StatementType.SCALE_IN_STORAGE_ENGINE;
    }

    public List<StorageEngine> getEngines() {
        return engines;
    }

    public void setEngines(StorageEngine engine) {
        this.engines.add(engine);
    }

    @Override
    public void execute(RequestContext ctx) {
        IginxWorker worker = IginxWorker.getInstance();
        ScaleInStorageEnginesReq req = new ScaleInStorageEnginesReq(ctx.getSessionId(), engines);
        ctx.setResult(new Result(worker.scaleInStorageEngines(req)));
    }
}
