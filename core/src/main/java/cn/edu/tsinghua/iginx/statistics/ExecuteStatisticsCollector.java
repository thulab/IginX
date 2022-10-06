package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.Result;
import cn.edu.tsinghua.iginx.engine.shared.processor.PostExecuteProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreExecuteProcessor;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.Statement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.entity.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ExecuteStatisticsCollector extends AbstractStageStatisticsCollector implements IExecuteStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteStatisticsCollector.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<StatementType, Pair<Long, Long>> detailInfos = new HashMap<>();
    private long count = 0;
    private long span = 0;
    private long queryPoints = 0;
    private long insertPoints = 0;

    @Override
    protected String getStageName() {
        return "ExecuteStage";
    }

    @Override
    protected void processStatistics(Statistics statistics) {
        lock.writeLock().lock();
        count += 1;
        span += statistics.getEndTime() - statistics.getStartTime();

        Statement statement = statistics.getContext().getStatement();
        Pair<Long, Long> detailInfo = detailInfos.computeIfAbsent(statement.getType(), e -> new Pair<>(0L, 0L));
        detailInfo.k += 1;
        detailInfo.v += statistics.getEndTime() - statistics.getStartTime();
        if (statement.getType() == StatementType.INSERT) {
            InsertStatement insertStatement = (InsertStatement) statement;
            insertPoints += (long) insertStatement.getTimes().size() * insertStatement.getPaths().size();
        }
        if (statement.getType() == StatementType.SELECT) {
            Result result = statistics.getContext().getResult();
            queryPoints += (long) result.getBitmapList().size() * result.getPaths().size();
        }
        lock.writeLock().unlock();
    }

    @Override
    public void broadcastStatistics() {
        lock.readLock().lock();
        logger.info("Execute Stage Statistics Info: ");
        logger.info("\tcount: " + count + ", span: " + span + "μs");
        if (count != 0) {
            logger.info("\taverage-span: " + (1.0 * span) / count + "μs");
        }
        for (Map.Entry<StatementType, Pair<Long, Long>> entry : detailInfos.entrySet()) {
            logger.info("\t\tFor Request: " + entry.getKey() + ", count: " + entry.getValue().k + ", span: " + entry.getValue().v + "μs");
        }
        logger.info("\ttotal insert points: " + insertPoints);
        logger.info("\ttotal query points: " + queryPoints);
        lock.readLock().unlock();
    }

    @Override
    public PreExecuteProcessor getPreExecuteProcessor() {
        return before::apply;
    }

    @Override
    public PostExecuteProcessor getPostExecuteProcessor() {
        return after::apply;
    }
}
