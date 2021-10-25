package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;

public abstract class Statement {

    public StatementType statementType = StatementType.NULL;

    public ExecuteSqlResp execute(long sessionId) {
        System.out.println("Abstract statement do nothing!");
        return null;
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
        SHOW_TIME_SERIES,
        SHOW_CLUSTER_INFO
    }
}
