package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class PythonTask extends Task {

    private String className = "";

    public PythonTask(TaskInfo info) {
        super(info);
        if (info.isSetClassName()) {
            className = info.getClassName();
        } else {
            throw new IllegalArgumentException("Python task must have class name.");
        }
    }

    public PythonTask(TaskFromYAML info) {
        super(info);
        if (info.getClassName() != null) {
            className = info.getClassName();
        } else {
            throw new IllegalArgumentException("Python task must have class name.");
        }
    }

    // for test
    public PythonTask(TaskType taskType, DataFlowType dataFlowType, long timeLimit,
                      String className) {
        super(taskType, dataFlowType, timeLimit);
        this.className = className;
    }

    public String getClassName() {
        return className;
    }
}
