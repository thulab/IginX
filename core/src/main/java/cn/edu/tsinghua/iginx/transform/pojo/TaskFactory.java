package cn.edu.tsinghua.iginx.transform.pojo;

import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;

public class TaskFactory {

    public static Task getTask(TaskInfo info) {
        TaskType taskType = info.getTaskType();

        if (taskType.equals(TaskType.IginX)) {
            return new IginXTask(info);
        } else if (taskType.equals(TaskType.Python)) {
            return new PythonTask(info);
        } else {
            throw new IllegalArgumentException("Unknown task type: " + taskType.toString());
        }
    }

    public static Task getTask(TaskFromYAML info) {
        String type = info.getTaskType().toLowerCase().trim();
        if (type.equals("iginx")) {
            return new IginXTask(info);
        } else if (type.equals("python")) {
            return new PythonTask(info);
        } else {
            throw new IllegalArgumentException("Unknown task type: " + type);
        }
    }
}
