package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.GetUserReq;
import cn.edu.tsinghua.iginx.thrift.GetUserResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;

import java.util.List;

public class ShowUserStatement extends Statement {

    private List<String> users;

    public ShowUserStatement(List<String> users) {
        this.users = users;
    }

    public List<String> getUsers() {
        return users;
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        IginxWorker worker = IginxWorker.getInstance();
        GetUserReq req = new GetUserReq(sessionId);
        if (users != null && !users.isEmpty()) {
            req.setUsernames(users);
        }
        GetUserResp getUserResp = worker.getUser(req);
        ExecuteSqlResp resp = new ExecuteSqlResp(getUserResp.getStatus(), SqlType.ShowUser);
        resp.setAuths(getUserResp.getAuths());
        resp.setUsernames(getUserResp.getUsernames());
        resp.setUserTypes(getUserResp.getUserTypes());
        return resp;
    }
}
