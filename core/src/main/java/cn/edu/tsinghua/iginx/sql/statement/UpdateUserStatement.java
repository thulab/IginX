package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.UpdateUserReq;

import java.util.Set;

public class UpdateUserStatement extends Statement {
    private String username;
    private String password;
    private Set<AuthType> authTypes;

    public UpdateUserStatement(String username, String password, Set<AuthType> authTypes) {
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
        UpdateUserReq req = new UpdateUserReq(sessionId, username);
        if (password != null) {
            req.setPassword(password);
        }
        if (authTypes != null) {
            req.setAuths(authTypes);
        }
        return new ExecuteSqlResp(worker.updateUser(req), SqlType.UpdateUser);
    }
}
