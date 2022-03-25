package cn.edu.tsinghua.iginx.sql.statement;

public class ClearDataStatement extends DataStatement {

  public ClearDataStatement() {
    this.statementType = StatementType.CLEAR_DATA;
  }
}
