package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.data.ExportWriter;

public class BatchStage implements Stage {

    private final DataFlowType dataFlowType;

    private final Stage beforeStage;

    private final Task task;

    private final ExportWriter exportWriter;

    public BatchStage(Stage beforeStage, Task task, ExportWriter exportWriter) {
        this.dataFlowType = DataFlowType.Batch;
        this.beforeStage = beforeStage;
        this.task = task;
        this.exportWriter = exportWriter;
    }

    public Stage getBeforeStage() {
        return beforeStage;
    }

    public Task getTask() {
        return task;
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
