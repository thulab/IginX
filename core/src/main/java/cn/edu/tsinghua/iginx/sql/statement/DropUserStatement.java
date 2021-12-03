package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.DeleteUserReq;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;

public class DropUserStatement extends Statement {

    private String username;

    public DropUserStatement(String username) {
        this.statementType = StatementType.DROP_USER;
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        IginxWorker worker = IginxWorker.getInstance();
        DeleteUserReq req = new DeleteUserReq(sessionId, username);
        return new ExecuteSqlResp(worker.deleteUser(req), SqlType.DropUser);
    }
}
