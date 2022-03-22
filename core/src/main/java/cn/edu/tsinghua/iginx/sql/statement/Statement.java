package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;

public abstract class Statement {

    public StatementType statementType = StatementType.NULL;

    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        throw new ExecutionException("this function should not be executed.");
    }

    public enum StatementType {
        NULL,
        SELECT,
        INSERT,
        DELETE,
        ADD_STORAGE_ENGINE,
        SHOW_REPLICATION,
        COUNT_POINTS,
        CLEAR_DATA,
        DELETE_TIME_SERIES,
        SHOW_TIMESERIES,
        SHOW_SUB_TIMESERIES,
        SHOW_CLUSTER_INFO,
        CREATE_USER,
        GRANT_USER,
        CHANGE_USER_PASSWORD,
        DROP_USER,
        SHOW_USER
    }
}
