package cn.edu.tsinghua.iginx.sql.statement;

public class InsertFromSelectStatement extends DataStatement {

    private final long timeOffset;

    private final SelectStatement subSelectStatement;

    private final InsertStatement subInsertStatement;

    public InsertFromSelectStatement(long timeOffset, SelectStatement subSelectStatement, InsertStatement subInsertStatement) {
        this.statementType = StatementType.INSERT_FROM_SELECT;
        this.timeOffset = timeOffset;
        this.subSelectStatement = subSelectStatement;
        this.subInsertStatement = subInsertStatement;
    }

    public long getTimeOffset() {
        return timeOffset;
    }

    public SelectStatement getSubSelectStatement() {
        return subSelectStatement;
    }

    public InsertStatement getSubInsertStatement() {
        return subInsertStatement;
    }
}
