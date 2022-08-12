package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class Task {

    private final TaskType taskType;

    private DataFlowType dataFlowType;

    private final long timeLimit;

    private long startTime;

    private long endTime;

    public Task(TaskInfo info) {
        taskType = info.getTaskType();
        dataFlowType = info.getDataFlowType();
        timeLimit = info.getTimeout();
    }

    public Task(TaskFromYAML info) {
        String type = info.getTaskType().toLowerCase().trim();
        switch (type) {
            case "iginx":
                taskType = TaskType.IginX;
                break;
            case "python":
                taskType = TaskType.Python;
                break;
            default:
                throw new IllegalArgumentException("Unknown task type: " + type);
        }

        dataFlowType = DataFlowType.Stream;
        if (info.getDataFlowType() != null) {
            type = info.getDataFlowType().toLowerCase().trim();
            switch (type) {
                case "batch":
                    dataFlowType = DataFlowType.Batch;
                    break;
                case "stream":
                    dataFlowType = DataFlowType.Stream;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown data flow type: " + type);
            }
        }
        timeLimit = info.getTimeout();
    }

    public Task(TaskType taskType, DataFlowType dataFlowType, long timeLimit) {
        this.taskType = taskType;
        this.dataFlowType = dataFlowType;
        this.timeLimit = timeLimit;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public DataFlowType getDataFlowType() {
        return dataFlowType;
    }

    public long getTimeLimit() {
        return timeLimit;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public boolean isPythonTask() {
        return taskType.equals(TaskType.Python);
    }
}
