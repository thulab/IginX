package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.monitor.NodeResource;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimulationBasedMigrationPolicy implements IMigrationPolicy {

  private ExecutorService executor;

  @Override
  public void migrate(List<MigrationTask> migrationTasks,
      Map<Long, NodeResource> nodeRestResourcesMap, double[] costParams) {
    executor = Executors.newCachedThreadPool();
    List<List<Queue<MigrationTask>>> candidateMigrationTaskQueueList = createParallelQueueByPriority(
        migrationTasks);

    double minUnbalance = Double.MAX_VALUE;
    int targetIndex = -1;
    for (int currIndex = 0;currIndex<candidateMigrationTaskQueueList.size() ; currIndex++) {
      List<Queue<MigrationTask>> migrationTaskQueueList = candidateMigrationTaskQueueList.get(currIndex);
      double currUnbalance = 0;
      while (!isAllQueueEmpty(migrationTaskQueueList)) {
        for(Queue<MigrationTask> migrationTaskQueue:migrationTaskQueueList){
          MigrationTask migrationTask = migrationTaskQueue.peek();
          //使用costParams，根据CPU、内存、磁盘、带宽判断是否能进行该任务
          if (canDo(migrationTask, costParams, nodeRestResourcesMap)) {
            //更改nodeRestResourcesMap
            currUnbalance+=calculateUnbalance(nodeRestResourcesMap) * migrationTask.getFragmentMeta().getNumOfPoints();
            migrationTaskQueue.poll();
          }
        }
      }
      if(currUnbalance < minUnbalance){
        minUnbalance = currUnbalance;
        targetIndex = currIndex;
      }
    }
  }

  private double calculateUnbalance(Map<Long, NodeResource> nodeRestResourcesMap){
    //计算不均衡值并返回
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

  private List<List<Queue<MigrationTask>>> createParallelQueueWithoutPriority(
      List<MigrationTask> migrationTasks) {
    return new ArrayList<>();
  }
}
