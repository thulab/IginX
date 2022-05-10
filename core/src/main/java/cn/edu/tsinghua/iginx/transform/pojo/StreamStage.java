package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.data.ExportWriter;

import java.util.List;

public class StreamStage implements Stage {

    private final DataFlowType dataFlowType;

    private final long sessionId;

    private final Stage beforeStage;

    private final List<Task> taskList;

    private final ExportWriter exportWriter;

    private final boolean startWithIginX;

    public StreamStage(long sessionId, Stage beforeStage, List<Task> taskList, ExportWriter writer) {
        this.dataFlowType = DataFlowType.Stream;
        this.sessionId = sessionId;
        this.beforeStage = beforeStage;
        this.taskList = taskList;
        this.exportWriter = writer;
        this.startWithIginX = taskList.get(0).getTaskType().equals(TaskType.IginX);
    }

    public long getSessionId() {
        return sessionId;
    }

    public Stage getBeforeStage() {
        return beforeStage;
    }

    public List<Task> getTaskList() {
        return taskList;
    }

    public boolean isStartWithIginX() {
        return startWithIginX;
    }

    @Override
    public DataFlowType getStageType() {
        return dataFlowType;
    }

    @Override
    public ExportWriter getExportWriter() {
        return exportWriter;
    }
}
