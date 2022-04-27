package cn.edu.tsinghua.iginx.utils;

public class TaskFromYAML {

    private String taskType;
    private String dataFlowType;
    private long timeout;
    private String fileName;
    private String className;
    private String sql;

    public TaskFromYAML() {

    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public String getDataFlowType() {
        return dataFlowType;
    }

    public void setDataFlowType(String dataFlowType) {
        this.dataFlowType = dataFlowType;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
