package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowTimeSeries;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.migration.recover.MigrationExecuteTask;
import cn.edu.tsinghua.iginx.migration.recover.MigrationExecuteType;
import cn.edu.tsinghua.iginx.migration.recover.MigrationLogger;
import cn.edu.tsinghua.iginx.migration.recover.MigrationLoggerAnalyzer;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;

public abstract class MigrationPolicy {

  protected ExecutorService executor;

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();

  private Logger logger;

  private final IPolicy policy = PolicyManager.getInstance()
      .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

  private MigrationLogger migrationLogger;

  private final static PhysicalEngine physicalEngine = PhysicalEngineImpl.getInstance();

  public MigrationPolicy(Logger logger) {
    this.logger = logger;
  }

  public void setMigrationLogger(MigrationLogger migrationLogger) {
    this.migrationLogger = migrationLogger;
  }

  public abstract void migrate(List<MigrationTask> migrationTasks,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap);

  /**
   * 在时间序列层面将分片在同一个du下分为两块
   */
  public void reshardByTimeseries(FragmentMeta fragmentMeta) {
    try {
      migrationLogger.logMigrationExecuteTaskStart(
          new MigrationExecuteTask(fragmentMeta, fragmentMeta.getMasterStorageUnitId(), 0L, 0L,
              MigrationExecuteType.RESHARD_TIME_SERIES));
      ShowTimeSeries showTimeSeries = new ShowTimeSeries(new GlobalSource(),
          fragmentMeta.getMasterStorageUnitId());
      RowStream rowStream = physicalEngine.execute(showTimeSeries);
      SortedSet<String> pathSet = new TreeSet<>();
      rowStream.getHeader().getFields().forEach(field -> {
        String timeSeries = field.getName();
        if (fragmentMeta.getTsInterval().isContain(timeSeries)) {
          pathSet.add(timeSeries);
        }
      });
      String middlePath = new ArrayList<>(pathSet).get(pathSet.size() / 2);
      fragmentMeta.endFragmentMetaByTimeSeries(middlePath);
      TimeSeriesInterval sourceTsInterval = new TimeSeriesInterval(
          fragmentMeta.getTsInterval().getStartTimeSeries(),
          fragmentMeta.getTsInterval().getEndTimeSeries());
      fragmentMeta.endFragmentMetaByTimeSeries(middlePath);
      DefaultMetaManager.getInstance().updateFragmentByTsInterval(sourceTsInterval, fragmentMeta);
      FragmentMeta newFragment = new FragmentMeta(middlePath,
          fragmentMeta.getTsInterval().getEndTimeSeries(),
          fragmentMeta.getTimeInterval().getStartTime(),
          fragmentMeta.getTimeInterval().getEndTime(), fragmentMeta.getMasterStorageUnit());
      DefaultMetaManager.getInstance().addFragment(newFragment);
    } catch (PhysicalException e) {
      logger.error("encounter error when reshard fragment by {} to {} ",
          fragmentMeta.getTsInterval().getStartTimeSeries(),
          fragmentMeta.getTsInterval().getEndTimeSeries(), e);
    } finally {
      migrationLogger.logMigrationExecuteTaskEnd();
    }
  }

  public void interrupt() {
    executor.shutdown();
  }

  public void recover() {
    try {
      MigrationLoggerAnalyzer migrationLoggerAnalyzer = new MigrationLoggerAnalyzer();
      migrationLoggerAnalyzer.analyze();
      if (migrationLoggerAnalyzer.isStartMigration() && !migrationLoggerAnalyzer
          .isMigrationFinished() && !migrationLoggerAnalyzer.isLastMigrationExecuteTaskFinished()) {
        MigrationExecuteTask migrationExecuteTask = migrationLoggerAnalyzer
            .getLastMigrationExecuteTask();
        if (migrationExecuteTask.getMigrationExecuteType() == MigrationExecuteType.MIGRATION) {
          FragmentMeta fragmentMeta = migrationExecuteTask.getFragmentMeta();
          // 直接删除整个du
          List<String> paths = new ArrayList<>();
          paths.add(migrationExecuteTask.getMasterStorageUnitId() + "*");
          Delete delete = new Delete(new FragmentSource(fragmentMeta), new ArrayList<>(), paths);
          physicalEngine.execute(delete);
        }
      }
    } catch (IOException | PhysicalException e) {
      e.printStackTrace();
    }

  }

  protected boolean canExecuteTargetMigrationTask(MigrationTask migrationTask,
      Map<Long, Long> nodeLoadMap) {
    long currTargetNodeLoad = nodeLoadMap.getOrDefault(migrationTask.getTargetStorageId(), 0L);
    return currTargetNodeLoad + migrationTask.getLoad() <= config.getMaxLoadThreshold();
  }

  protected boolean isAllQueueEmpty(List<Queue<MigrationTask>> migrationTaskQueueList) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      if (!migrationTaskQueue.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  protected void sortQueueListByFirstItem(List<Queue<MigrationTask>> migrationTaskQueue) {
    migrationTaskQueue
        .sort((o1, o2) -> {
          MigrationTask migrationTask1 = o1.peek();
          MigrationTask migrationTask2 = o2.peek();
          if (migrationTask1 == null || migrationTask2 == null) {
            return 1;
          } else {
            return (int) (migrationTask2.getPriorityScore() - migrationTask1.getPriorityScore());
          }
        });
  }

  protected Map<Long, Long> calculateNodeLoadMap(Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap) {
    Map<Long, Long> nodeLoadMap = new HashMap<>();
    for (Entry<Long, List<FragmentMeta>> nodeFragmentEntry : nodeFragmentMap.entrySet()) {
      List<FragmentMeta> fragmentMetas = nodeFragmentEntry.getValue();
      for (FragmentMeta fragmentMeta : fragmentMetas) {
        nodeLoadMap.put(nodeFragmentEntry.getKey(),
            fragmentWriteLoadMap.getOrDefault(fragmentMeta, 0L) + fragmentReadLoadMap
                .getOrDefault(fragmentMeta, 0L));
      }
    }
    return nodeLoadMap;
  }

  protected synchronized void executeOneRoundMigration(
      List<Queue<MigrationTask>> migrationTaskQueueList,
      Map<Long, Long> nodeLoadMap) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      MigrationTask migrationTask = migrationTaskQueue.peek();
      //根据负载判断是否能进行该任务
      if (migrationTask != null && canExecuteTargetMigrationTask(migrationTask, nodeLoadMap)) {
        migrationTaskQueue.poll();
        sortQueueListByFirstItem(migrationTaskQueueList);
        this.executor.submit(() -> {
          //异步执行耗时的操作
          if (migrationTask.getMigrationType() == MigrationType.QUERY) {
            // 如果之前没切过分区，需要优先切一下分区
            if (migrationTask.getFragmentMeta().getTimeInterval().getEndTime() == Long.MAX_VALUE) {
              FragmentMeta fragmentMeta = reshardFragment(migrationTask.getSourceStorageId(),
                  migrationTask.getTargetStorageId(),
                  migrationTask.getFragmentMeta());
              migrationTask.setFragmentMeta(fragmentMeta);
            }
            migrateData(migrationTask.getSourceStorageId(),
                migrationTask.getTargetStorageId(),
                migrationTask.getFragmentMeta());
          } else {
            reshardFragment(migrationTask.getSourceStorageId(),
                migrationTask.getTargetStorageId(),
                migrationTask.getFragmentMeta());
          }
          this.logger
              .info("complete one migration task from {} to {} with load: {}, size: {}, type: {}",
                  migrationTask.getSourceStorageId(), migrationTask.getTargetStorageId(),
                  migrationTask.getLoad(), migrationTask.getSize(),
                  migrationTask.getMigrationType());
          // 执行下一轮判断
          while (!isAllQueueEmpty(migrationTaskQueueList)) {
            executeOneRoundMigration(migrationTaskQueueList, nodeLoadMap);
          }
        });
        migrationTaskQueue.poll();
      }
    }
  }

  private void migrateData(long sourceStorageId, long targetStorageId,
      FragmentMeta fragmentMeta) {
    try {
      // 在目标节点创建新du
      StorageUnitMeta storageUnitMeta;
      try {
        storageUnitMeta = DefaultMetaManager.getInstance()
            .generateNewStorageUnitMetaByFragment(fragmentMeta, targetStorageId);
      } catch (MetaStorageException e) {
        logger.error("cannot create storage unit in target storage engine", e);
        throw new PhysicalException(e);
      }
      migrationLogger.logMigrationExecuteTaskStart(
          new MigrationExecuteTask(fragmentMeta, storageUnitMeta.getId(), sourceStorageId,
              targetStorageId,
              MigrationExecuteType.MIGRATION));
      // 开始迁移数据
      Migration migration = new Migration(new GlobalSource(), sourceStorageId, targetStorageId,
          fragmentMeta, storageUnitMeta);
      physicalEngine.execute(migration);
      // 迁移完开始删除原数据
      List<String> paths = new ArrayList<>();
      paths.add(fragmentMeta.getMasterStorageUnitId() + "*");
      List<TimeRange> timeRanges = new ArrayList<>();
      timeRanges.add(new TimeRange(fragmentMeta.getTimeInterval().getStartTime(), true,
          fragmentMeta.getTimeInterval().getEndTime(), false));
      Delete delete = new Delete(new FragmentSource(fragmentMeta), timeRanges, paths);
      physicalEngine.execute(delete);
    } catch (Exception e) {
      logger.error("encounter error when migrate data from {} to {} ", sourceStorageId,
          targetStorageId, e);
    } finally {
      migrationLogger.logMigrationExecuteTaskEnd();
    }
  }

  private FragmentMeta reshardFragment(long sourceStorageId, long targetStorageId,
      FragmentMeta fragmentMeta) {
    try {
      migrationLogger.logMigrationExecuteTaskStart(
          new MigrationExecuteTask(fragmentMeta, fragmentMeta.getMasterStorageUnitId(),
              sourceStorageId, targetStorageId,
              MigrationExecuteType.RESHARD_TIME));
      // [startTime, +∞) & (startPath, endPath)
      TimeSeriesInterval tsInterval = fragmentMeta.getTsInterval();
      TimeInterval timeInterval = fragmentMeta.getTimeInterval();
      List<Long> storageEngineList = new ArrayList<>();
      storageEngineList.add(targetStorageId);

      // 排除乱序写入问题
      if (timeInterval.getEndTime() == Long.MAX_VALUE) {
        operateTaskAndRequest(sourceStorageId, targetStorageId, fragmentMeta);
        Pair<FragmentMeta, StorageUnitMeta> fragmentMetaStorageUnitMetaPair = policy
            .generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
                tsInterval.getStartTimeSeries(), tsInterval.getEndTimeSeries(),
                DefaultMetaManager.getInstance().getMaxActiveEndTime(), Long.MAX_VALUE,
                storageEngineList);
        return DefaultMetaManager.getInstance()
            .splitFragmentAndStorageUnit(fragmentMetaStorageUnitMetaPair.getV(),
                fragmentMetaStorageUnitMetaPair.getK(), fragmentMeta);
      }
      return null;
    } finally {
      migrationLogger.logMigrationExecuteTaskEnd();
    }
  }

  private void operateTaskAndRequest(long sourceStorageId, long targetStorageId,
      FragmentMeta fragmentMeta) {
    // TODO 暂时先不管迁移过程中的请求问题
  }

}
