package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.transform.api.Stage;
import cn.edu.tsinghua.iginx.transform.data.*;
import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Job {

    private long jobId;

    private long sessionId;

    private JobState state;
    private long startTime;
    private long endTime;

    private boolean needExport;
    private ExportType exportType;
    private ExportWriter writer;

    private final List<Task> taskList;
    private final List<Stage> stageList;

    public Job(long id, CommitTransformJobReq req) {
        jobId = id;
        sessionId = req.getSessionId();
        state = JobState.JOB_CREATED;

        exportType = req.getExportType();
        if (exportType.equals(ExportType.File)) {
            needExport = true;
            writer = new FileAppendWriter(req.getFileName());
        } else if (exportType.equals(ExportType.IginX)) {
            needExport = true;
            writer = new IginXWriter(req.getSessionId());
        } else {
            needExport = false;
            writer = new LogWriter();
        }

        taskList = new ArrayList<>();
        stageList = new ArrayList<>();
        Stage stage = null;
        List<Task> stageTasks = new ArrayList<>();
        for (int i = 0; i < req.getTaskListSize(); i++) {
            TaskInfo info = req.getTaskList().get(i);
            Task task = TaskFactory.getTask(info);
            taskList.add(task);

            if (task.getDataFlowType().equals(DataFlowType.Batch)) {
                if (!stageTasks.isEmpty()) {
                    stage = new StreamStage(sessionId, stage, new ArrayList<>(stageTasks), new CollectionWriter());
                    stageList.add(stage);
                    stageTasks.clear();
                }
                if (i == req.getTaskListSize() - 1) {
                    stage = new BatchStage(stage, task, writer);
                } else {
                    stage = new BatchStage(stage, task, new CollectionWriter());
                }
                stageList.add(stage);
            } else {
                stageTasks.add(task);
            }
        }
        if (!stageTasks.isEmpty()) {
            stage = new StreamStage(sessionId, stage, new ArrayList<>(stageTasks), writer);
            stageList.add(stage);
        }
    }

    public Job(long id, long sessionId, JobFromYAML jobFromYAML) {
        this.jobId = id;
        this.sessionId = sessionId;
        this.state = JobState.JOB_CREATED;

        String exportType = jobFromYAML.getExportType().toLowerCase().trim();
        if (exportType.equals("file")) {
            this.exportType = ExportType.File;
            this.needExport = true;
            this.writer = new FileAppendWriter(jobFromYAML.getExportFile());
        } else if (exportType.equals("iginx")) {
            this.exportType = ExportType.IginX;
            this.needExport = true;
            this.writer = new IginXWriter(sessionId);
        } else {
            this.exportType = ExportType.None;
            this.needExport = false;
            this.writer = new LogWriter();
        }

        taskList = new ArrayList<>();
        stageList = new ArrayList<>();
        Stage stage = null;
        List<Task> stageTasks = new ArrayList<>();
        for (int i = 0; i < jobFromYAML.getTaskList().size(); i++) {
            TaskFromYAML taskFromYAML = jobFromYAML.getTaskList().get(i);
            Task task = TaskFactory.getTask(taskFromYAML);
            taskList.add(task);

            if (task.getDataFlowType().equals(DataFlowType.Batch)) {
                if (!stageTasks.isEmpty()) {
                    stage = new StreamStage(sessionId, stage, new ArrayList<>(stageTasks), new CollectionWriter());
                    stageList.add(stage);
                    stageTasks.clear();
                }
                if (i == jobFromYAML.getTaskList().size() - 1) {
                    stage = new BatchStage(stage, task, writer);
                } else {
                    stage = new BatchStage(stage, task, new CollectionWriter());
                }
                stageList.add(stage);
            } else {
                stageTasks.add(task);
            }
        }
        if (!stageTasks.isEmpty()) {
            stage = new StreamStage(sessionId, stage, new ArrayList<>(stageTasks), writer);
            stageList.add(stage);
        }
    }

    @Override
    public String toString() {
        return "Job{" +
            "jobId=" + jobId +
            ", sessionId=" + sessionId +
            ", state=" + state +
            ", startTime=" + startTime +
            ", endTime=" + endTime +
            ", needExport=" + needExport +
            ", exportType=" + exportType +
            ", writer=" + writer +
            ", taskList=" + taskList +
            ", stageList=" + stageList +
            '}';
    }
}
