package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimulationBasedMigrationPolicy extends MigrationPolicy {

  private static final Logger logger = LoggerFactory
      .getLogger(SimulationBasedMigrationPolicy.class);

  public SimulationBasedMigrationPolicy(Logger logger) {
    super(logger);
  }

  @Override
  public void migrate(List<MigrationTask> migrationTasks,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap) {
    long startTime = System.currentTimeMillis();

    Map<Long, Long> nodeLoadMap = calculateNodeLoadMap(nodeFragmentMap, fragmentWriteLoadMap,
        fragmentReadLoadMap);
    List<List<Queue<MigrationTask>>> candidateMigrationTaskQueueList = createParallelQueueWithoutPriority(
        migrationTasks);

    // 模拟迁移过程找出最高效的一组
    double minUnbalance = Double.MAX_VALUE;
    int targetIndex = -1;
    for (int currIndex = 0; currIndex < candidateMigrationTaskQueueList.size(); currIndex++) {
      List<Queue<MigrationTask>> migrationTaskQueueList = candidateMigrationTaskQueueList
          .get(currIndex);
      // 模拟正在进行的迁移任务, 任务->起始时间
      Map<MigrationTask, Long> workingMigrationTask = new HashMap<>();
      long currTime = 0L;
      double currUnbalance = 0;
      while (!isAllQueueEmpty(migrationTaskQueueList)) {
        simulateOneRoundMigration(migrationTaskQueueList, nodeLoadMap, workingMigrationTask,
            currTime);
        // 算出一下个被执行完的任务，并更新时间以及不均衡度
        MigrationTask nextFinishedMigrationTask = getNextFinishedMigrationTask(
            workingMigrationTask);
        long finishedTime = nextFinishedMigrationTask.getMigrationSize() + workingMigrationTask
            .get(nextFinishedMigrationTask);
        currUnbalance += calculateUnbalance(nodeLoadMap) * (finishedTime - currTime);
        currTime = finishedTime;
      }
      if (currUnbalance < minUnbalance) {
        minUnbalance = currUnbalance;
        targetIndex = currIndex;
      }
    }
    logger.info("complete simulate migration task with time consumption: {} ms",
        System.currentTimeMillis() - startTime);

    List<Queue<MigrationTask>> migrationTaskQueueList = candidateMigrationTaskQueueList
        .get(targetIndex);
    executor = Executors.newCachedThreadPool();
    while (!isAllQueueEmpty(migrationTaskQueueList)) {
      executeOneRoundMigration(migrationTaskQueueList, nodeLoadMap);
    }

    logger.info("complete all migration task with time consumption: {} ms",
        System.currentTimeMillis() - startTime);
  }

  private synchronized void simulateOneRoundMigration(
      List<Queue<MigrationTask>> migrationTaskQueueList,
      Map<Long, Long> nodeLoadMap, Map<MigrationTask, Long> workingMigrationTask, long currTime) {
    for (Queue<MigrationTask> migrationTaskQueue : migrationTaskQueueList) {
      MigrationTask migrationTask = migrationTaskQueue.peek();
      //根据负载判断是否能进行该任务
      if (migrationTask != null && canExecuteTargetMigrationTask(migrationTask, nodeLoadMap)) {
        migrationTaskQueue.poll();
        sortQueueListByFirstItem(migrationTaskQueueList);
        workingMigrationTask.put(migrationTask, currTime);
      }
    }
  }

  /**
   * 计算不均衡值并返回（当前为标准差）
   */
  private double calculateUnbalance(Map<Long, Long> nodeLoadMap) {
    return MigrationUtils.variance(nodeLoadMap.values());
  }

  private MigrationTask getNextFinishedMigrationTask(Map<MigrationTask, Long> migrationTasks) {
    MigrationTask nextFinishedMigrationTask = null;
    long minFinishedTime = Long.MAX_VALUE;
    for (Entry<MigrationTask, Long> migrationTasksEntry : migrationTasks.entrySet()) {
      MigrationTask migrationTask = migrationTasksEntry.getKey();
      long startTime = migrationTasksEntry.getValue();
      long finishedTime = startTime + migrationTask.getMigrationSize();
      if (finishedTime < minFinishedTime) {
        minFinishedTime = finishedTime;
        nextFinishedMigrationTask = migrationTask;
      }
    }
    migrationTasks.remove(nextFinishedMigrationTask);
    return nextFinishedMigrationTask;
  }

  @Override
  public void recover() {

  }

  private List<List<Queue<MigrationTask>>> createParallelQueueWithoutPriority(
      List<MigrationTask> migrationTasks) {
    Map<String, List<MigrationTask>> edgeMigrationTasksMap = new HashMap<>();
    for (MigrationTask migrationTask : migrationTasks) {
      String edgeId =
          migrationTask.getSourceStorageId() + "-" + migrationTask.getTargetStorageId();
      List<MigrationTask> edgeMigrationTasks = edgeMigrationTasksMap
          .computeIfAbsent(edgeId, k -> new ArrayList<>());
      edgeMigrationTasks.add(migrationTask);
    }

    List<List<List<MigrationTask>>> migrationTaskLists = new ArrayList<>();
    for (Entry<String, List<MigrationTask>> edgeMigrationTasksEntry : edgeMigrationTasksMap
        .entrySet()) {
      List<MigrationTask> edgeMigrationTasks = edgeMigrationTasksEntry.getValue();
      //获取该列表的所有组合形式
      List<List<MigrationTask>> allPossibleMigrationTaskList = MigrationUtils
          .combination(edgeMigrationTasks, edgeMigrationTasks.size());
      migrationTaskLists.add(allPossibleMigrationTaskList);
    }

    //乘法原理获取所有可能的排列组合
    int[] listEndStatus = new int[migrationTaskLists.size()];
    int[] listCurrIndex = new int[migrationTaskLists.size()];
    for (int i = 0; i < migrationTaskLists.size(); i++) {
      List<List<MigrationTask>> edgeMigrationTaskList = migrationTaskLists.get(i);
      listEndStatus[i] = edgeMigrationTaskList.size() - 1;
      listCurrIndex[i] = 0;
    }

    List<List<Queue<MigrationTask>>> candidateMigrationTaskQueueList = new ArrayList<>();
    while (!Arrays.equals(listEndStatus, listCurrIndex)) {
      for (int i = listCurrIndex.length - 1; i >= 0; i--) {
        if (listCurrIndex[i] < listEndStatus[i]) {
          listCurrIndex[i]++;
          for (int j = i + 1; j < listCurrIndex.length; j++) {
            listCurrIndex[j] = 0;
          }
          break;
        }
      }

      List<Queue<MigrationTask>> migrationTaskQueueList = new ArrayList<>();
      for (int i = 0; i < listCurrIndex.length; i++) {
        Queue<MigrationTask> migrationTaskQueue = new LinkedBlockingQueue<>(
            migrationTaskLists.get(i).get(listCurrIndex[i]));
        migrationTaskQueueList.add(migrationTaskQueue);
      }
      sortQueueListByFirstItem(migrationTaskQueueList);
      candidateMigrationTaskQueueList.add(migrationTaskQueueList);
    }

    logger.info("start to simulate migration task of {} possibilities",
        candidateMigrationTaskQueueList.size());
    return candidateMigrationTaskQueueList;
  }

}
