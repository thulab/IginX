package cn.edu.tsinghua.iginx.sql.statement;

public class ShowTimeSeriesStatement extends DataStatement {

    public ShowTimeSeriesStatement() {
        this.statementType = StatementType.SHOW_TIME_SERIES;
    }
}
