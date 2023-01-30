package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.IginxWorker;
import cn.edu.tsinghua.iginx.engine.shared.RequestContext;
import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.AddStorageEnginesReq;
import cn.edu.tsinghua.iginx.thrift.RemoveHistoryDataSourceReq;

import java.util.ArrayList;
import java.util.List;

public class RemoveHsitoryDataSourceStatement extends SystemStatement {

    private List<Long> storageIDList;

    public List<Long> getStorageID() {
        return storageIDList;
    }

    public void setStorageID(List<Long> storageID) {
        this.storageIDList = storageID;
    }

    public void addStorageID(Long storageID) {
        this.storageIDList.add(storageID);
    }

    public RemoveHsitoryDataSourceStatement() {
        storageIDList = new ArrayList<>();
        this.statementType = StatementType.REMOVE_HISTORY_DATA_RESOURCE;
    }

    @Override
    public void execute(RequestContext ctx) throws ExecutionException {
        IginxWorker worker = IginxWorker.getInstance();
        RemoveHistoryDataSourceReq req = new RemoveHistoryDataSourceReq(ctx.getSessionId(), storageIDList);
        ctx.setResult(new Result(worker.removeHistoryDataSource(req)));
    }
}
