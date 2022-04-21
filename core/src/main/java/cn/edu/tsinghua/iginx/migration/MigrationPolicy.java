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
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;

public abstract class MigrationPolicy {

  protected ExecutorService executor;

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();

  private Logger logger;

  private final IPolicy policy = PolicyManager.getInstance()
      .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

  private final static PhysicalEngine physicalEngine = PhysicalEngineImpl.getInstance();

  public MigrationPolicy(Logger logger) {
    this.logger = logger;
  }

  public abstract void migrate(List<MigrationTask> migrationTasks,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap);

  public void interrupt() {
    executor.shutdown();
  }

  public abstract void recover();

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
              reshardFragment(migrationTask.getSourceStorageId(),
                  migrationTask.getTargetStorageId(),
                  migrationTask.getFragmentMeta());
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
      // 开始迁移数据
      Migration migration = new Migration(new GlobalSource(), sourceStorageId, targetStorageId,
          fragmentMeta);
      RowStream rowStream = physicalEngine.execute(migration);
      // 设置分片现在所属的storageId
      fragmentMeta.getMasterStorageUnit().setStorageEngineId(targetStorageId);
      // 迁移完开始删除原数据
      List<String> paths = new ArrayList<>();
      rowStream.getHeader().getFields().forEach(field -> paths.add(field.getName()));
      List<TimeRange> timeRanges = new ArrayList<>();
      timeRanges.add(new TimeRange(fragmentMeta.getTimeInterval().getStartTime(), true,
          fragmentMeta.getTimeInterval().getEndTime(), false));
      Delete delete = new Delete(new FragmentSource(fragmentMeta), timeRanges, paths);
      physicalEngine.execute(delete);
    } catch (PhysicalException e) {
      logger.error("encounter error when migrate data from {} to {}", sourceStorageId,
          targetStorageId);
    }
  }

  private void reshardFragment(long sourceStorageId, long targetStorageId,
      FragmentMeta fragmentMeta) {
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
      DefaultMetaManager.getInstance()
          .createFragmentAndStorageUnit(fragmentMetaStorageUnitMetaPair.getV(),
              fragmentMetaStorageUnitMetaPair.getK());
      // 切完分区要更新现在元数据里的统计信息

    }
  }

  private void operateTaskAndRequest(long sourceStorageId, long targetStorageId,
      FragmentMeta fragmentMeta) {
    // TODO 暂时先不管迁移过程中的请求问题
  }

}
