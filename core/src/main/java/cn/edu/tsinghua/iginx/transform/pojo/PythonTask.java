package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class PythonTask extends Task {

    private String fileName = "";

    private String className = "";

    public PythonTask(TaskInfo info) {
        super(info);
        if (info.isSetFileName() && info.isSetClassName()) {
            fileName = info.getFileName();
            className = info.getClassName();
        } else {
            throw new IllegalArgumentException("Python task must have file name and class name.");
        }
    }

    public PythonTask(TaskFromYAML info) {
        super(info);
        if (info.getFileName() != null && info.getClassName() != null) {
            fileName = info.getFileName();
            className = info.getClassName();
        } else {
            throw new IllegalArgumentException("Python task must have file name and class name.");
        }
    }

    // for test
    public PythonTask(TaskType taskType, DataFlowType dataFlowType, long timeLimit,
                      String fileName, String className) {
        super(taskType, dataFlowType, timeLimit);
        this.fileName = fileName;
        this.className = className;
    }

    public String getFileName() {
        return fileName;
    }

    public String getClassName() {
        return className;
    }
}
