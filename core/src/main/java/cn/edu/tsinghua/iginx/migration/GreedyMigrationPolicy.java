package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.monitor.NodeResource;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class GreedyMigrationPolicy implements IMigrationPolicy {

  private ExecutorService executor;

  @Override
  public void migrate(List<MigrationTask> migrationTasks,
      Map<Long, NodeResource> nodeRestResourcesMap, double[] costParams) {
    List<Queue<MigrationTask>> migrationTaskQueueList = createParallelQueueByPriority(
        migrationTasks);
    executor = Executors.newCachedThreadPool();

    while (!isAllQueueEmpty(migrationTaskQueueList)) {
      executeOneRoundMigration(migrationTaskQueueList, nodeRestResourcesMap, costParams);
    }
  }

  private void executeOneRoundMigration(List<Queue<MigrationTask>> migrationTaskQueueList,
      Map<Long, NodeResource> nodeRestResourcesMap, double[] costParams) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      MigrationTask migrationTask = migrationTaskQueue.peek();
      //使用costParams，根据CPU、内存、磁盘、带宽判断是否能进行该任务
      if (canDo(migrationTask, costParams, nodeRestResourcesMap)) {
        executor.submit(() -> {
          //异步执行耗时的操作
          if (migrationTask.getMigrationType() == MigrationType.QUERY) {
            MigrationUtils
                .migrateData(migrationTask.getSourceNodeId(), migrationTask.getTargetNodeId(),
                    migrationTask.getFragmentMeta());
          } else {
            MigrationUtils
                .reshardFragment(migrationTask.getSourceNodeId(),
                    migrationTask.getTargetNodeId(),
                    migrationTask.getFragmentMeta());
          }
          executeNextRoundMigration(migrationTaskQueueList, nodeRestResourcesMap, costParams);
        });
        migrationTaskQueue.poll();
      }
    }
  }

  private void executeNextRoundMigration(List<Queue<MigrationTask>> migrationTaskQueueList,
      Map<Long, NodeResource> nodeRestResourcesMap, double[] costParams) {
    while (!isAllQueueEmpty(migrationTaskQueueList)) {
      executeOneRoundMigration(migrationTaskQueueList, nodeRestResourcesMap, costParams);
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
