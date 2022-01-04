package cn.edu.tsinghua.iginx.sql.statement;

import java.util.ArrayList;
import java.util.List;

public class DeleteTimeSeriesStatement extends DataStatement {

    private List<String> paths;

    public DeleteTimeSeriesStatement() {
        this.statementType = StatementType.DELETE_TIME_SERIES;
        paths = new ArrayList<>();
    }

    public List<String> getPaths() {
        return paths;
    }

    public void addPath(String path) {
        this.paths.add(path);
    }
}
