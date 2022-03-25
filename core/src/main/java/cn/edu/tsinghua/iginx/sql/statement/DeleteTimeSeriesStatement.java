package cn.edu.tsinghua.iginx.sql.statement;

import java.util.ArrayList;
import java.util.List;

public class DeleteTimeSeriesStatement extends DataStatement {

  private List<String> paths;

  public DeleteTimeSeriesStatement() {
    this.statementType = StatementType.DELETE_TIME_SERIES;
    this.paths = new ArrayList<>();
  }

  public DeleteTimeSeriesStatement(List<String> paths) {
    this.statementType = StatementType.DELETE_TIME_SERIES;
    this.paths = paths;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void addPath(String path) {
    this.paths.add(path);
  }
}
