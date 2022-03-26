package cn.edu.tsinghua.iginx.sql.statement;

public abstract class Statement {

    public StatementType statementType = StatementType.NULL;

    public StatementType getType() {
        return statementType;
    }

}
