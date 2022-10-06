package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.entity.TaskFromYAML;

import java.util.ArrayList;
import java.util.List;

public class IginXTask extends Task {

    private final List<String> sqlList = new ArrayList<>();

    public IginXTask(TaskInfo info) {
        super(info);
        if (info.isSetSqlList()) {
            sqlList.addAll(info.getSqlList());
        } else {
            throw new IllegalArgumentException("IginX task must have a SQL statement.");
        }
    }

    public IginXTask(TaskFromYAML info) {
        super(info);
        if (info.getSqlList() != null && !info.getSqlList().isEmpty()) {
            sqlList.addAll(info.getSqlList());
        } else {
            throw new IllegalArgumentException("IginX task must have a SQL statement.");
        }
    }

    public List<String> getSqlList() {
        return sqlList;
    }
}
