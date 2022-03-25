package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.engine.shared.processor.PostLogicalProcessor;
import cn.edu.tsinghua.iginx.engine.shared.processor.PreLogicalProcessor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalStatisticsCollector extends AbstractStageStatisticsCollector implements
    ILogicalStatisticsCollector {

  private static final Logger logger = LoggerFactory.getLogger(LogicalStatisticsCollector.class);
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private long count = 0;
  private long span = 0;

  @Override
  protected String getStageName() {
    return "LogicalStage";
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
    logger.info("Logical Stage Statistics Info: ");
    logger.info("\tcount: " + count + ", span: " + span + "μs");
    if (count != 0) {
      logger.info("\taverage-span: " + (1.0 * span) / count + "μs");
    }
    lock.readLock().unlock();
  }

  @Override
  public PreLogicalProcessor getPreLogicalProcessor() {
    return before::apply;
  }

  @Override
  public PostLogicalProcessor getPostLogicalProcessor() {
    return after::apply;
  }
}
