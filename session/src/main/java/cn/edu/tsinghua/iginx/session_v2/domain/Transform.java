package cn.edu.tsinghua.iginx.session_v2.domain;

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.thrift.ExportType;

import java.util.ArrayList;
import java.util.List;

public class Transform {

    private final List<Task> taskList;

    private final ExportType exportType;

    private final String fileName;

    private final List<String> exportNameList;

    public Transform(List<Task> taskList, ExportType exportType, String fileName, List<String> exportNameList) {
        this.taskList = taskList;
        this.exportType = exportType;
        this.fileName = fileName;
        this.exportNameList = exportNameList;
    }

    public Transform(Transform.Builder builder) {
        this(builder.taskList, builder.exportType, builder.fileName, builder.exportNameList);
    }

    public static Transform.Builder builder() {
        return new Transform.Builder();
    }

    public List<Task> getTaskList() {
        return taskList;
    }

    public ExportType getExportType() {
        return exportType;
    }

    public String getFileName() {
        return fileName;
    }

    public List<String> getExportNameList() {
        return exportNameList;
    }

    public static class Builder {

        private List<Task> taskList = new ArrayList<>();

        private ExportType exportType;

        private String fileName;

        private List<String> exportNameList;

        public Transform.Builder addTask(Task task) {
            taskList.add(task);
            return this;
        }

        public Transform.Builder exportToLog() {
            this.exportType = ExportType.Log;
            return this;
        }

        public Transform.Builder exportToIginX() {
            return exportToIginX(exportNameList);
        }

        public Transform.Builder exportToIginX(List<String> exportNameList) {
            this.exportType = ExportType.IginX;
            this.exportNameList = exportNameList;
            return this;
        }

        public Transform.Builder exportToFile(String fileName) {
            return exportToFile(fileName, null);
        }

        public Transform.Builder exportToFile(String fileName, List<String> exportNameList) {
            this.exportType = ExportType.File;
            this.fileName = fileName;
            this.exportNameList = exportNameList;
            return this;
        }

        public Transform build() {
            Arguments.checkListNonEmpty(taskList, "taskList");
            Arguments.checkNotNull(exportType, "exportType");
            return new Transform(this);
        }
    }
}
