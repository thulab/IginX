package cn.edu.tsinghua.iginx.sql.statement;

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
