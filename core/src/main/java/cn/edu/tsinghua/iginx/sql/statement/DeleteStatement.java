package cn.edu.tsinghua.iginx.sql.statement;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;

import java.util.ArrayList;
import java.util.List;

public class DeleteStatement extends DataStatement {

    private List<String> paths;
    private Filter filter;

    public DeleteStatement() {
        this.statementType = StatementType.DELETE;
        paths = new ArrayList<>();
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void addPath(String path) {
        paths.add(path);
    }

    @Override
    public ExecuteSqlResp execute(long sessionId) throws ExecutionException {
        throw new ExecutionException("Select statement can not be executed directly.");
    }
}
