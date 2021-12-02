package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.thrift.UpdateUserReq;

import java.util.Set;

public class GrantUserStatement extends Statement {
    private String username;
    private Set<AuthType> authTypes;

    public GrantUserStatement(String username, Set<AuthType> authTypes) {
        this.statementType = StatementType.GRANT_USER;
        this.username = username;
        this.authTypes = authTypes;
    }

    public String getUsername() {
        return username;
    }

    public Set<AuthType> getAuthTypes() {
        return authTypes;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        IginxWorker worker = IginxWorker.getInstance();
        UpdateUserReq req = new UpdateUserReq(sessionId, username);
        if (authTypes != null && !authTypes.isEmpty()) {
            req.setAuths(authTypes);
        }
        return new ExecuteSqlResp(worker.updateUser(req), SqlType.GrantUser);
    }
}
