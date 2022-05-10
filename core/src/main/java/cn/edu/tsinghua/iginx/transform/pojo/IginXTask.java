package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class IginXTask extends Task {

    private String sql = "";

    public IginXTask(TaskInfo info) {
        super(info);
        if (info.isSetSql()) {
            sql = info.getSql();
        } else {
            throw new IllegalArgumentException("IginX task must have a SQL statement.");
        }
    }

    public IginXTask(TaskFromYAML info) {
        super(info);
        if (info.getSql() != null) {
            sql = info.getSql();
        } else {
            throw new IllegalArgumentException("IginX task must have a SQL statement.");
        }
    }

    public String getSql() {
        return sql;
    }
}
