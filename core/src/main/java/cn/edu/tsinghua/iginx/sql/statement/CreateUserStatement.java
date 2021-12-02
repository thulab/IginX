package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.AddUserReq;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;

import java.util.HashSet;
import java.util.Set;

public class CreateUserStatement extends Statement {

    private String username;
    private String password;

    public CreateUserStatement(String username, String password) {
        this.statementType = StatementType.CREATE_USER;
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
        AddUserReq req = new AddUserReq(sessionId, username, password, new HashSet<>());
        return new ExecuteSqlResp(worker.addUser(req), SqlType.CreateUser);
    }
}
