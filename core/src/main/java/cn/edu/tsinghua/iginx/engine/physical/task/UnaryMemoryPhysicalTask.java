package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.query.expression.Operator;

import java.util.List;

public class UnaryMemoryPhysicalTask extends MemoryPhysicalTask {

    private final PhysicalTask parentTask;

    public UnaryMemoryPhysicalTask(List<Operator> operators, PhysicalTask parentTask) {
        super(operators);
        this.parentTask = parentTask;
    }

    public PhysicalTask getParentTask() {
        return parentTask;
    }

    @Override
    public TaskType getType() {
        return TaskType.UnaryMemory;
    }

    @Override
    public TaskExecuteResult execute() {
        return null;
    }
}
