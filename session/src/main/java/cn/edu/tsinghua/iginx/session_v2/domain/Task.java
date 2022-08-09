package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskType;

import java.util.ArrayList;
import java.util.List;

public class Task {

    private final TaskType taskType;

    private final DataFlowType dataFlowType;

    private final long timeout;

    private final List<String> sqlList;

    private final String pyTaskName;

    public Task(TaskType taskType, DataFlowType dataFlowType, long timeout, List<String> sqlList, String pyTaskName) {
        this.taskType = taskType;
        this.dataFlowType = dataFlowType;
        this.timeout = timeout;
        this.sqlList = sqlList;
        this.pyTaskName = pyTaskName;
    }

    public Task(Task.Builder builder) {
        this(builder.taskType, builder.dataFlowType, builder.timeout, builder.sqlList, builder.pyTaskName);
    }

    public static Task.Builder builder() {
        return new Task.Builder();
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public DataFlowType getDataFlowType() {
        return dataFlowType;
    }

    public long getTimeout() {
        return timeout;
    }

    public List<String> getSqlList() {
        return sqlList;
    }

    public String getPyTaskName() {
        return pyTaskName;
    }

    public static class Builder {

        private TaskType taskType;

        private DataFlowType dataFlowType;

        private long timeout = Long.MAX_VALUE;

        private List<String> sqlList = new ArrayList<>();

        private String pyTaskName;

        public Task.Builder dataFlowType(DataFlowType dataFlowType) {
            this.dataFlowType = dataFlowType;
            return this;
        }

        public Task.Builder timeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Task.Builder sql(String sql) {
            Arguments.checkTaskType(TaskType.IginX, taskType);
            this.taskType = TaskType.IginX;
            this.sqlList.add(sql);
            return this;
        }

        public Task.Builder pyTaskName(String pyTaskName) {
            Arguments.checkTaskType(TaskType.Python, taskType);
            this.taskType = TaskType.Python;
            this.pyTaskName = pyTaskName;
            return this;
        }

        public Task build() {
            Arguments.checkNotNull(taskType, "taskType");
            Arguments.checkNotNull(dataFlowType, "dataFlowType");
            return new Task(this);
        }
    }
}
