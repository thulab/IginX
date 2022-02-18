package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostParseProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreParseProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ParseStatisticsCollector extends AbstractStageStatisticsCollector implements IParseStatisticsCollector {

    private static final Logger logger = LoggerFactory.getLogger(ParseStatisticsCollector.class);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private long count = 0;
    private long span = 0;

    @Override
    protected String getStageName() {
        return "ParseSQLStage";
    }

    @Override
    protected void processStatistics(Statistics statistics) {
        lock.writeLock().lock();
        count += 1;
        span += statistics.getEndTime() - statistics.getStartTime();
        lock.writeLock().unlock();
    }

    @Override
    public void broadcastStatistics() {
        lock.readLock().lock();
        logger.info("Parse Stage Statistics Info: ");
        logger.info("\tcount: " + count + ", span: " + span + "μs");
        if (count != 0) {
            logger.info("\taverage-span: " + (1.0 * span) / count + "μs");
        }
        lock.readLock().unlock();
    }

    @Override
    public PreParseProcessor getPreParseProcessor() {
        return before::apply;
    }

    @Override
    public PostParseProcessor getPostParseProcessor() {
        return after::apply;
    }
}
