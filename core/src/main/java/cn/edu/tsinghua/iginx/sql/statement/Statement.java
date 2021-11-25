package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;

public abstract class Statement {

    public StatementType statementType = StatementType.NULL;

    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        throw new ExecutionException("");
    }

    public StatementType getType() {
        return statementType;
    }

}
