package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationType;
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

  public MigrationPolicy(Logger logger) {
    this.logger = logger;
  }

  public abstract void migrate(List<MigrationTask> migrationTasks,
      Map<String, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap);

  public void interrupt() {
    executor.shutdown();
  }

  public abstract void recover();

  protected boolean canExecuteTargetMigrationTask(MigrationTask migrationTask,
      Map<String, Long> nodeLoadMap) {
    long currTargetNodeLoad = nodeLoadMap.getOrDefault(migrationTask.getTargetStorageUnitId(), 0L);
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

  protected Map<String, Long> calculateNodeLoadMap(Map<String, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap) {
    Map<String, Long> nodeLoadMap = new HashMap<>();
    for (Entry<String, List<FragmentMeta>> nodeFragmentEntry : nodeFragmentMap.entrySet()) {
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
      Map<String, Long> nodeLoadMap) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      MigrationTask migrationTask = migrationTaskQueue.peek();
      //根据负载判断是否能进行该任务
      if (migrationTask != null && canExecuteTargetMigrationTask(migrationTask, nodeLoadMap)) {
        migrationTaskQueue.poll();
        sortQueueListByFirstItem(migrationTaskQueueList);
        this.executor.submit(() -> {
          //异步执行耗时的操作
          if (migrationTask.getMigrationType() == MigrationType.QUERY) {
            MigrationUtils
                .migrateData(migrationTask.getSourceStorageUnitId(),
                    migrationTask.getTargetStorageUnitId(),
                    migrationTask.getFragmentMeta());
          } else {
            MigrationUtils
                .reshardFragment(migrationTask.getSourceStorageUnitId(),
                    migrationTask.getTargetStorageUnitId(),
                    migrationTask.getFragmentMeta());
          }
          this.logger
              .info("complete one migration task from {} to {} with load: {}, size: {}, type: {}",
                  migrationTask.getSourceStorageUnitId(), migrationTask.getTargetStorageUnitId(),
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
}
