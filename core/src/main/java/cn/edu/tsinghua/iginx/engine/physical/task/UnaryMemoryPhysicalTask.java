package cn.edu.tsinghua.iginx.engine.physical.task;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutorFactory;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class UnaryMemoryPhysicalTask extends MemoryPhysicalTask {

    private static final Logger logger = LoggerFactory.getLogger(UnaryMemoryPhysicalTask.class);

    private final PhysicalTask parentTask;

    public UnaryMemoryPhysicalTask(List<Operator> operators, PhysicalTask parentTask) {
        super(TaskType.UnaryMemory, operators);
        this.parentTask = parentTask;
    }

    public PhysicalTask getParentTask() {
        return parentTask;
    }

    @Override
    public TaskExecuteResult execute() {
        TaskExecuteResult parentResult = parentTask.getResult();
        if (parentResult == null) {
            return new TaskExecuteResult(new PhysicalException("unexpected parent task execute result for " + this + ": null"));
        }
        if (parentResult.getException() != null) {
            return parentResult;
        }
        List<Operator> operators = getOperators();
        RowStream stream = parentResult.getRowStream();
        OperatorMemoryExecutor executor = OperatorMemoryExecutorFactory.getInstance().getMemoryExecutor();
        try {
            for (Operator op : operators) {
                if (OperatorType.isBinaryOperator(op.getType())) {
                    throw new UnexpectedOperatorException("unexpected binary operator " + op + " in unary task");
                }
                stream = executor.executeUnaryOperator((UnaryOperator) op, stream);
            }
        } catch (PhysicalException e) {
            logger.error("encounter error when execute operator in memory: ", e);
            return new TaskExecuteResult(e);
        }
        return new TaskExecuteResult(stream);
    }

    @Override
    public boolean notifyParentReady() {
        return parentReadyCount.incrementAndGet() == 1;
    }
}
