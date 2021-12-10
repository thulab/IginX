package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;

public class SystemStatement extends Statement {

    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        throw new ExecutionException("");
    }
}
