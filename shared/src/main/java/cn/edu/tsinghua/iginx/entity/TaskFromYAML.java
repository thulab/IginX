package cn.edu.tsinghua.iginx.entity;

import java.util.ArrayList;
import java.util.List;

public class TaskFromYAML {

    private String taskType;
    private String dataFlowType;
    private long timeout;
    private String pyTaskName;
    private List<String> sqlList;

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

    public String getPyTaskName() {
        return pyTaskName;
    }

    public void setPyTaskName(String pyTaskName) {
        this.pyTaskName = pyTaskName;
    }

    public List<String> getSqlList() {
        return sqlList;
    }

    public void setSqlList(List<String> sqlList) {
        this.sqlList = sqlList;
    }
}
