package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.thrift.AddStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import java.util.ArrayList;
import java.util.List;

public class AddStorageEngineStatement extends SystemStatement {

  private final List<StorageEngine> engines;

  public AddStorageEngineStatement() {
    engines = new ArrayList<>();
    this.statementType = StatementType.ADD_STORAGE_ENGINE;
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
    AddStorageEnginesReq req = new AddStorageEnginesReq(ctx.getSessionId(), engines);
    ctx.setResult(new Result(worker.addStorageEngines(req)));
  }
}
