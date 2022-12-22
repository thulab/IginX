package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 目前专门用于 CombineNonQuery 操作符
 */
public class MultipleMemoryPhysicalTask extends MemoryPhysicalTask {

    private static final Logger logger = LoggerFactory.getLogger(MultipleMemoryPhysicalTask.class);

    private final List<PhysicalTask> parentTasks;

    public MultipleMemoryPhysicalTask(List<Operator> operators, List<PhysicalTask> parentTasks) {
        super(TaskType.MultipleMemory, operators);
        this.parentTasks = parentTasks;
    }

    public List<PhysicalTask> getParentTasks() {
        return parentTasks;
    }

    @Override
    public TaskExecuteResult execute() {
        List<Operator> operators = getOperators();
        if (operators.size() != 1) {
            return new TaskExecuteResult(new PhysicalException("unexpected multiple memory physical task"));
        }
        Operator operator = operators.get(0);
        if (operator.getType() != OperatorType.CombineNonQuery) {
            return new TaskExecuteResult(new PhysicalException("unexpected multiple memory physical task"));
        }
        if (getFollowerTask() != null) {
            return new TaskExecuteResult(new PhysicalException("multiple memory physical task shouldn't have follower task"));
        }
        List<PhysicalException> exceptions = new ArrayList<>();
        for (PhysicalTask parentTask : parentTasks) {
            PhysicalException exception = parentTask.getResult().getException();
            if (exception != null) {
                exceptions.add(exception);
            }
        }
        if (exceptions.size() != 0) {
            StringBuilder message = new StringBuilder("some sub-task execute failure, details: ");
            for (PhysicalException exception : exceptions) {
                message.append(exception.getMessage());
            }
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException(message.toString()));
        }
        return new TaskExecuteResult();
    }

    @Override
    public boolean notifyParentReady() {
        return parentReadyCount.incrementAndGet() == parentTasks.size();
    }
}
