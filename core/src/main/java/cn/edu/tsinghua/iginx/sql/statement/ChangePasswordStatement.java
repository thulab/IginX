package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.UpdateUserReq;

public class ChangePasswordStatement extends Statement {

    private String username;
    private String password;

    public ChangePasswordStatement(String username, String password) {
        this.statementType = StatementType.CHANGE_USER_PASSWORD;
        this.username = username;
        this.password = password;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        IginxWorker worker = IginxWorker.getInstance();
        UpdateUserReq req = new UpdateUserReq(sessionId, username);
        if (password != null) {
            req.setPassword(password);
        }
        return new ExecuteSqlResp(worker.updateUser(req), SqlType.ChangeUserPassword);
    }
}
