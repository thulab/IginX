package cn.edu.tsinghua.iginx.sql.operator;

import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;

public abstract class Operator {

    public OperatorType operatorType = OperatorType.NULL;

    public ExecuteSqlResp doOperation(long sessionId) {
        System.out.println("Operator!");
        return null;
    }

    public enum OperatorType {
        NULL,
        SELECT,
        INSERT,
        DELETE,
        ADD_STORAGE_ENGINE,
        SHOW_REPLICATION,
        COUNT_POINTS,
        CLEAR_DATA,
    }
}
