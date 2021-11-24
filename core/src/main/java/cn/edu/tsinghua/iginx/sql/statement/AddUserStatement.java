package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.AddUserReq;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;

import java.util.Set;

public class AddUserStatement extends Statement {

    private String username;
    private String password;
    private Set<AuthType> authTypes;

    public AddUserStatement(String username, String password, Set<AuthType> authTypes) {
        this.statementType = StatementType.ADD_USER;
        this.username = username;
        this.password = password;
        this.authTypes = authTypes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Set<AuthType> getAuthTypes() {
        return authTypes;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        IginxWorker worker = IginxWorker.getInstance();
        AddUserReq req = new AddUserReq(sessionId, username, password, authTypes);
        return new ExecuteSqlResp(worker.addUser(req), SqlType.AddUser);
    }
}
