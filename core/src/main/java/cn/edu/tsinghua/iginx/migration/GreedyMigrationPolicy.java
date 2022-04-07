package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GreedyMigrationPolicy implements IMigrationPolicy {

  private ExecutorService executor;
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(GreedyMigrationPolicy.class);

  @Override
  public void migrate(List<MigrationTask> migrationTasks,
      Map<String, List<FragmentMeta>> nodeFragmentMap, Map<FragmentMeta, Long> fragmentWriteLoadMap,
      Map<FragmentMeta, Long> fragmentReadLoadMap) {
    List<Queue<MigrationTask>> migrationTaskQueueList = createParallelQueueByPriority(
        migrationTasks);
    executor = Executors.newCachedThreadPool();

    while (!isAllQueueEmpty(migrationTaskQueueList)) {
      executeOneRoundMigration(migrationTaskQueueList, nodeRestResourcesMap);
    }
  }

  private void executeOneRoundMigration(List<Queue<MigrationTask>> migrationTaskQueueList,
      Map<Long, NodeResource> nodeRestResourcesMap) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      MigrationTask migrationTask = migrationTaskQueue.peek();
      //根据CPU、内存、磁盘、带宽判断是否能进行该任务
      if (canExecuteTargetMigrationTask(migrationTask, nodeRestResourcesMap)) {
        executor.submit(() -> {
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
          executeNextRoundMigration(migrationTaskQueueList, nodeRestResourcesMap, costParams);
        });
        migrationTaskQueue.poll();
      }
    }
  }

  private boolean canExecuteTargetMigrationTask(MigrationTask migrationTask,
      Map<Long, NodeResource> nodeRestResourcesMap) {

  }

  private void executeNextRoundMigration(List<Queue<MigrationTask>> migrationTaskQueueList,
      Map<Long, NodeResource> nodeRestResourcesMap) {
    while (!isAllQueueEmpty(migrationTaskQueueList)) {
      executeOneRoundMigration(migrationTaskQueueList, nodeRestResourcesMap);
    }
  }

  private boolean isAllQueueEmpty(List<Queue<MigrationTask>> migrationTaskQueueList) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      if (!migrationTaskQueue.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void interrupt() {

  }

  @Override
  public void recover() {

  }

  private List<Queue<MigrationTask>> createParallelQueueByPriority(
      List<MigrationTask> migrationTasks) {
    return new ArrayList<>();
  }
}
