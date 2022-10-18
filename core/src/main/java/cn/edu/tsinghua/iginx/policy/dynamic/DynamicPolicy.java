package cn.edu.tsinghua.iginx.policy.dynamic;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.migration.MigrationManager;
import cn.edu.tsinghua.iginx.migration.MigrationTask;
import cn.edu.tsinghua.iginx.migration.MigrationType;
import cn.edu.tsinghua.iginx.migration.recover.MigrationLogger;
import cn.edu.tsinghua.iginx.monitor.TimeseriesMonitor;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.Utils;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import lpsolve.LpSolve;
import lpsolve.LpSolveException;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DynamicPolicy implements IPolicy {

  protected AtomicBoolean needReAllocate = new AtomicBoolean(false);
  private IMetaManager iMetaManager;
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(DynamicPolicy.class);
  private static final double unbalanceFinalStatusThreshold = config
      .getUnbalanceFinalStatusThreshold();
  private static final int timeseriesloadBalanceCheckInterval = ConfigDescriptor.getInstance()
      .getConfig().getTimeseriesloadBalanceCheckInterval();
  private static final double maxTimeseriesLoadBalanceThreshold = ConfigDescriptor.getInstance()
      .getConfig().getMaxTimeseriesLoadBalanceThreshold();

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();

  @Override
  public void notify(DataStatement statement) {
    if (statement.getType() == StatementType.INSERT) {
      InsertStatement insertStatement = (InsertStatement) statement;
      iMetaManager.saveTimeSeriesData(insertStatement);
      DefaultMetaManager.getInstance().updateMaxActiveEndTime(insertStatement.getEndTime());
    }
  }

  @Override
  public void init(IMetaManager iMetaManager) {
    this.iMetaManager = iMetaManager;
    StorageEngineChangeHook hook = getStorageEngineChangeHook();
    if (hook != null) {
      iMetaManager.registerStorageEngineChangeHook(hook);
    }
  }

  @Override
  public StorageEngineChangeHook getStorageEngineChangeHook() {
    return (before, after) -> {
      // 哪台机器加了分片，哪台机器初始化，并且在批量添加的时候只有最后一个存储引擎才会导致扩容发生
      if (before == null && after != null && after.getCreatedBy() == iMetaManager.getIginxId() && after.isNeedReAllocate()) {
        needReAllocate.set(true);
        logger.info("新的可写节点进入集群，集群需要重新分片");
      }
      // TODO: 针对节点退出的情况缩容
    };
  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(
      DataStatement statement) {
    List<String> paths = Utils.getPathListFromStatement(statement);
    TimeInterval timeInterval = new TimeInterval(0, Long.MAX_VALUE);

    if (ConfigDescriptor.getInstance().getConfig().getClients().indexOf(",") > 0) {
      Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> pair = generateInitialFragmentsAndStorageUnitsByClients(
          paths, timeInterval);
      return new Pair<>(pair.k.values().stream().flatMap(List::stream).collect(Collectors.toList()),
          pair.v);
    } else {
      return generateInitialFragmentsAndStorageUnitsDefault(paths, timeInterval);
    }
  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(DataStatement statement) {
    logger.debug("dynamic policy do not have to reallocate fragments by one statement");
    return new Pair<>(new ArrayList<>(), new ArrayList<>());
  }

  /**
   * This storage unit initialization method is used when clients are provided, such as in TPCx-IoT
   * tests
   */
  public Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnitsByClients(
      List<String> paths, TimeInterval timeInterval) {
    Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = new HashMap<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();

    List<StorageEngineMeta> storageEngineList = iMetaManager.getStorageEngineList();
    int storageEngineNum = storageEngineList.size();

    String[] clients = ConfigDescriptor.getInstance().getConfig().getClients().split(",");
    int instancesNumPerClient =
        ConfigDescriptor.getInstance().getConfig().getInstancesNumPerClient() - 1;
    int replicaNum = Math
        .min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
    String[] prefixes = new String[clients.length * instancesNumPerClient];
    for (int i = 0; i < clients.length; i++) {
      for (int j = 0; j < instancesNumPerClient; j++) {
        prefixes[i * instancesNumPerClient + j] = clients[i] + (j + 2);
      }
    }
    Arrays.sort(prefixes);

    List<FragmentMeta> fragmentMetaList;
    String masterId;
    StorageUnitMeta storageUnit;
    for (int i = 0; i < clients.length * instancesNumPerClient - 1; i++) {
      fragmentMetaList = new ArrayList<>();
      masterId = RandomStringUtils.randomAlphanumeric(16);
      storageUnit = new StorageUnitMeta(masterId,
          storageEngineList.get(i % storageEngineNum).getId(), masterId, true);
      for (int j = i + 1; j < i + replicaNum; j++) {
        storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16),
            storageEngineList.get(j % storageEngineNum).getId(), masterId, false));
      }
      storageUnitList.add(storageUnit);
      fragmentMetaList
          .add(new FragmentMeta(prefixes[i], prefixes[i + 1], 0, Long.MAX_VALUE, masterId));
      fragmentMap.put(new TimeSeriesInterval(prefixes[i], prefixes[i + 1]), fragmentMetaList);
    }

    fragmentMetaList = new ArrayList<>();
    masterId = RandomStringUtils.randomAlphanumeric(16);
    storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(0).getId(), masterId, true);
    for (int i = 1; i < replicaNum; i++) {
      storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16),
          storageEngineList.get(i).getId(), masterId, false));
    }
    storageUnitList.add(storageUnit);
    fragmentMetaList.add(new FragmentMeta(null, prefixes[0], 0, Long.MAX_VALUE, masterId));
    fragmentMap.put(new TimeSeriesInterval(null, prefixes[0]), fragmentMetaList);

    fragmentMetaList = new ArrayList<>();
    masterId = RandomStringUtils.randomAlphanumeric(16);
    storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(storageEngineNum - 1).getId(),
        masterId, true);
    for (int i = 1; i < replicaNum; i++) {
      storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16),
          storageEngineList.get(storageEngineNum - 1 - i).getId(), masterId, false));
    }
    storageUnitList.add(storageUnit);
    fragmentMetaList.add(
        new FragmentMeta(prefixes[clients.length * instancesNumPerClient - 1], null, 0,
            Long.MAX_VALUE, masterId));
    fragmentMap
        .put(new TimeSeriesInterval(prefixes[clients.length * instancesNumPerClient - 1], null),
            fragmentMetaList);

    return new Pair<>(fragmentMap, storageUnitList);
  }

  /**
   * This storage unit initialization method is used when no information about workloads is
   * provided
   */
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnitsDefault(
      List<String> inspaths, TimeInterval timeInterval) {
    List<FragmentMeta> fragmentList = new ArrayList<>();
    List<StorageUnitMeta> storageUnitList = new ArrayList<>();
    List<String> paths = new ArrayList<>();
    if (inspaths.size() > 0) {
      paths.add(inspaths.get(0));
    }

    if (inspaths.size() > 1) {
      paths.add(inspaths.get(inspaths.size() - 1));
    }

    int storageEngineNum = iMetaManager.getStorageEngineNum();
    int replicaNum = Math
        .min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
    List<Long> storageEngineIdList;
    Pair<FragmentMeta, StorageUnitMeta> pair;
    int index = 0;

    // [0, startTime) & (-∞, +∞)
    // 一般情况下该范围内几乎无数据，因此作为一个分片处理
    if (timeInterval.getStartTime() != 0) {
      storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
      pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(null, null, 0,
          timeInterval.getStartTime(), storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
    }

    // [startTime, +∞) & (null, startPath)
    storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
    pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(null, paths.get(0),
        timeInterval.getStartTime(), Long.MAX_VALUE, storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

    // [startTime, +∞) & [startPath, endPath)
    int splitNum = Math.max(Math.min(storageEngineNum, paths.size() - 1), 0);
    for (int i = 0; i < splitNum; i++) {
      storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
      pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
          paths.get(i * (paths.size() - 1) / splitNum),
          paths.get((i + 1) * (paths.size() - 1) / splitNum), timeInterval.getStartTime(),
          Long.MAX_VALUE, storageEngineIdList);
      fragmentList.add(pair.k);
      storageUnitList.add(pair.v);
    }

    // [startTime, +∞) & [endPath, null)
    storageEngineIdList = generateStorageEngineIdList(index, replicaNum);
    pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
        paths.get(paths.size() - 1), null, timeInterval.getStartTime(), Long.MAX_VALUE,
        storageEngineIdList);
    fragmentList.add(pair.k);
    storageUnitList.add(pair.v);

    return new Pair<>(fragmentList, storageUnitList);
  }

  @Override
  public Pair<FragmentMeta, StorageUnitMeta> generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
      String startPath, String endPath, long startTime, long endTime,
      List<Long> storageEngineList) {
    String masterId = RandomStringUtils.randomAlphanumeric(16);
    StorageUnitMeta storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(0), masterId,
        true, false);
    FragmentMeta fragment = new FragmentMeta(startPath, endPath, startTime, endTime, masterId);
    for (int i = 1; i < storageEngineList.size(); i++) {
      storageUnit.addReplica(
          new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineList.get(i),
              masterId, false, false));
    }
    return new Pair<>(fragment, storageUnit);
  }

  private List<Long> generateStorageEngineIdList(int startIndex, int num) {
    List<Long> storageEngineIdList = new ArrayList<>();
    List<StorageEngineMeta> storageEngines = iMetaManager.getStorageEngineList();
    for (int i = startIndex; i < startIndex + num; i++) {
      storageEngineIdList.add(storageEngines.get(i % storageEngines.size()).getId());
    }
    return storageEngineIdList;
  }

  public boolean isNeedReAllocate() {
    return needReAllocate.getAndSet(false);
  }

  public void setNeedReAllocate(boolean needReAllocate) {
    this.needReAllocate.set(needReAllocate);
  }

  @Override
  public void executeReshardAndMigration(
      Map<FragmentMeta, Long> fragmentMetaPointsMap,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap,
      List<Long> toScaleInNodes) {
    MigrationLogger migrationLogger = new MigrationLogger();
    MigrationManager.getInstance().getMigration().setMigrationLogger(migrationLogger);

    // 平均资源占用
    double totalHeat = 0;
    for (long heat : fragmentWriteLoadMap.values()) {
      totalHeat += heat;
    }
    for (long heat : fragmentReadLoadMap.values()) {
      totalHeat += heat;
    }
    int totalFragmentNum = 0;
    Map<Long, Long> nodeLoadMap = new HashMap<>();
    for (Entry<Long, List<FragmentMeta>> nodeFragmentEntry : nodeFragmentMap.entrySet()) {
      totalFragmentNum += nodeFragmentEntry.getValue().size();
      long nodeTotalHeat = 0L;
      for (FragmentMeta fragmentMeta : nodeFragmentEntry.getValue()) {
        nodeTotalHeat += fragmentWriteLoadMap.getOrDefault(fragmentMeta, 0L);
        nodeTotalHeat += fragmentReadLoadMap.getOrDefault(fragmentMeta, 0L);
      }
      nodeLoadMap.put(nodeFragmentEntry.getKey(), nodeTotalHeat);
    }

    // 确定m矩阵，原来分区在节点上的分布
    int[][] m = new int[totalFragmentNum][nodeFragmentMap.size()];
    for (int i = 0; i < totalFragmentNum; i++) {
      for (int j = 0; j < nodeFragmentMap.size(); j++) {
        m[i][j] = 1;
      }
    }
    // 过滤掉原本就在该节点上的分区
    int currNode = 0;
    int cumulativeSize = 0;
    for (List<FragmentMeta> fragmentMetas : nodeFragmentMap.values()) {
      int currSize = fragmentMetas.size();
      for (int i = cumulativeSize; i < cumulativeSize + currSize; i++) {
        m[i][currNode] = 0;
      }
      cumulativeSize += fragmentMetas.size();
      currNode++;
    }

    // 无法找到迁移方案的情况，可能是因为分区过大了，则根据序列拆分分区
    double maxLoad = totalHeat / nodeFragmentMap.size() * (1 + unbalanceFinalStatusThreshold);
    long maxWriteLoad = 0L;
    FragmentMeta maxWriteLoadFragment = null;
    long maxReadLoad = 0L;
    FragmentMeta maxReadLoadFragment = null;
    for (Entry<FragmentMeta, Long> fragmentWriteLoadEntry : fragmentWriteLoadMap.entrySet()) {
      long load = fragmentWriteLoadEntry.getValue();
      if (maxWriteLoad < load) {
        maxWriteLoadFragment = fragmentWriteLoadEntry.getKey();
        maxWriteLoad = load;
      }
    }
    for (Entry<FragmentMeta, Long> fragmentReadLoadEntry : fragmentReadLoadMap.entrySet()) {
      long load = fragmentReadLoadEntry.getValue();
      if (maxReadLoad < load) {
        maxReadLoadFragment = fragmentReadLoadEntry.getKey();
        maxReadLoad = load;
      }
    }
    if (maxWriteLoadFragment != null || maxReadLoadFragment != null) {
      if (maxLoad < Math.max(maxWriteLoad, maxReadLoad)) {
        logger.info("start to execute timeseries reshard");
        if (maxWriteLoad >= maxReadLoad) {
          executeTimeseriesReshard(maxWriteLoadFragment,
              fragmentMetaPointsMap.get(maxWriteLoadFragment), nodeLoadMap, true);
        } else {
//          executeTimeseriesReshard(maxReadLoadFragment,
//              fragmentMetaPointsMap.get(maxReadLoadFragment), nodeLoadMap, false);
        }
        logger.info("end execute timeseries reshard");
        return;
      }
    }

    logger.error("start to calculate migration final status");
    int[][][] migrationResults = calculateMigrationFinalStatus(totalHeat / nodeFragmentMap.size(),
        fragmentMetaPointsMap, nodeFragmentMap, fragmentWriteLoadMap, fragmentReadLoadMap,
        totalFragmentNum, m, toScaleInNodes);
    logger.error("end calculate migration final status");

    // 所有的分区
    FragmentMeta[] allFragmentMetas = new FragmentMeta[totalFragmentNum];
    int currIndex = 0;
    for (List<FragmentMeta> fragmentMetas : nodeFragmentMap.values()) {
      for (FragmentMeta fragmentMeta : fragmentMetas) {
        allFragmentMetas[currIndex] = fragmentMeta;
        currIndex++;
      }
    }
    // 所有的节点
    Long[] allNodes = new Long[nodeFragmentMap.size()];
    nodeFragmentMap.keySet().toArray(allNodes);
    // 计算迁移计划
    List<MigrationTask> migrationTasks = new ArrayList<>();
    for (int i = 0; i < totalFragmentNum; i++) {
      FragmentMeta fragmentMeta = allFragmentMetas[i];
      for (int j = 0; j < allNodes.length; j++) {
        // 只找迁移的源节点
        if (m[i][j] == 0) {
          // 写要迁移
          if (migrationResults[0][i][j] == 0) {
            int targetIndex;
            for (targetIndex = 0; targetIndex < allNodes.length; targetIndex++) {
              if (migrationResults[0][i][targetIndex] == 1) {
                break;
              }
            }
            if (targetIndex == allNodes.length) {
              continue;
            }
            migrationTasks.add(new MigrationTask(fragmentMeta,
                fragmentWriteLoadMap.getOrDefault(fragmentMeta, 0L),
                fragmentMetaPointsMap
                    .getOrDefault(fragmentMeta, MigrationTask.RESHARD_MIGRATION_COST), allNodes[j],
                allNodes[targetIndex],
                MigrationType.WRITE));
          }
          // 读要迁移
          else if (migrationResults[1][i][j] == 0) {
            int targetIndex;
            for (targetIndex = 0; targetIndex < allNodes.length; targetIndex++) {
              if (migrationResults[1][i][targetIndex] == 1) {
                break;
              }
            }
            if (targetIndex == allNodes.length) {
              continue;
            }
            migrationTasks.add(new MigrationTask(fragmentMeta,
                fragmentReadLoadMap.getOrDefault(fragmentMeta, 0L),
                fragmentMetaPointsMap
                    .getOrDefault(fragmentMeta, MigrationTask.RESHARD_MIGRATION_COST), allNodes[j],
                allNodes[targetIndex],
                MigrationType.QUERY));
          }
        }
      }
    }
    logger.error("start to print migration task:");
    for (MigrationTask migrationTask : migrationTasks) {
      logger.error(migrationTask.toString());
    }
    logger.error("end print migration task:");

    migrationLogger.logMigrationTasks(migrationTasks);
    // 执行迁移
    MigrationManager.getInstance().getMigration()
        .migrate(migrationTasks, nodeFragmentMap, fragmentWriteLoadMap, fragmentReadLoadMap);
  }

  private void executeTimeseriesReshard(FragmentMeta fragmentMeta, long points,
      Map<Long, Long> storageHeat, boolean isWrite) {
    try {
      // 如果是写入则不需要考虑可定制化副本的情况
      if (isWrite) {
        MigrationManager.getInstance().getMigration()
            .reshardWriteByTimeseries(fragmentMeta, points);
      } else {
        TimeseriesMonitor.getInstance().start();
        Thread.sleep(timeseriesloadBalanceCheckInterval * 1000L);
        TimeseriesMonitor.getInstance().stop();
        metaManager.updateTimeseriesHeat(TimeseriesMonitor.getInstance().getTimeseriesLoadMap());
        //等待收集完成
        while (!metaManager.isAllTimeseriesMonitorsCompleteCollection()) {
          Thread.sleep(1000);
        }
        long totalHeat = 0L;
        Map<String, Long> timeseriesHeat = metaManager.loadTimeseriesHeat();
        for (Entry<String, Long> timeseriesHeatEntry : timeseriesHeat.entrySet()) {
          totalHeat += timeseriesHeatEntry.getValue();
        }

        double averageHeat = totalHeat * 1.0 / timeseriesHeat.size();
        Map<String, Long> overLoadTimeseriesMap = new HashMap<>();
        for (Entry<String, Long> timeseriesHeatEntry : timeseriesHeat.entrySet()) {
          if (timeseriesHeatEntry.getValue() > averageHeat * (1
              + maxTimeseriesLoadBalanceThreshold)) {
            overLoadTimeseriesMap.put(timeseriesHeatEntry.getKey(), timeseriesHeatEntry.getValue());
          }
        }

        if (overLoadTimeseriesMap.size() > 0
            && fragmentMeta.getTimeInterval().getEndTime() != Long.MAX_VALUE && !isWrite) {
//          MigrationManager.getInstance().getMigration()
//              .reshardByCustomizableReplica(fragmentMeta, timeseriesHeat,
//                  overLoadTimeseriesMap.keySet(), totalHeat, points, storageHeat);
        } else {
          MigrationManager.getInstance().getMigration()
              .reshardQueryByTimeseries(fragmentMeta, timeseriesHeat);
        }
      }
    } catch (Exception e) {
      logger.error("execute timeseries reshard failed :", e);
    } finally {
      //完成一轮负载均衡
      DefaultMetaManager.getInstance().doneReshard();
    }
  }

  private int[][][] calculateMigrationFinalStatus(double averageScore,
      Map<FragmentMeta, Long> fragmentMetaPointsMap,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap,
      int totalFragmentNum, int[][] m, List<Long> toScaleInNodes) {
    //分区大小函数
    long[] fragmentSize = new long[totalFragmentNum];
    long[] writeLoad = new long[totalFragmentNum];
    long[] readLoad = new long[totalFragmentNum];
    List<Integer> toScaleInNodeIndexList = new ArrayList<>();
    int currNodeIndex = 0;
    int currIndex = 0;
    for (Entry<Long, List<FragmentMeta>> nodeFragmentEntry : nodeFragmentMap.entrySet()) {
      if (toScaleInNodes.contains(nodeFragmentEntry.getKey())) {
        toScaleInNodeIndexList.add(currNodeIndex);
      }
      List<FragmentMeta> fragmentMetas = nodeFragmentEntry.getValue();
      for (FragmentMeta fragmentMeta : fragmentMetas) {
        fragmentSize[currIndex] = fragmentMetaPointsMap
            .getOrDefault(fragmentMeta, MigrationTask.RESHARD_MIGRATION_COST);
        writeLoad[currIndex] = fragmentWriteLoadMap.getOrDefault(fragmentMeta, 0L);
        readLoad[currIndex] = fragmentReadLoadMap.getOrDefault(fragmentMeta, 0L);
        currIndex++;
      }
      currNodeIndex++;
    }

    int[][][] result = new int[2][totalFragmentNum][nodeFragmentMap.size()];

    int lpParamNum = 2 * totalFragmentNum * nodeFragmentMap.size();
    int res = LpSolve.INFEASIBLE;
    LpSolve problem = null;

    try {
      int loopTime = 0;
      while (res != LpSolve.OPTIMAL && 1 >= loopTime * 0.1 + unbalanceFinalStatusThreshold) {
        double maxLoad = averageScore * (1 + unbalanceFinalStatusThreshold + 0.1 * loopTime);
        double minLoad = averageScore * (1 - unbalanceFinalStatusThreshold - 0.1 * loopTime);
        // 声明lp_solve优化模型
        problem = LpSolve.makeLp(0, lpParamNum);
        problem.setMinim();
        // 设置为0,1变量
        for (int i = 1; i <= lpParamNum; i++) {
          problem.setBinary(i, true);
        }

        // 定义目标函数
        double[] objFnList = new double[lpParamNum + 1];
        for (int i = 0; i < totalFragmentNum; i++) {
          for (int j = 0; j < nodeFragmentMap.size(); j++) {
            if (m[i][j] == 1) {
              objFnList[i * nodeFragmentMap.size() + j + 1] = MigrationTask.RESHARD_MIGRATION_COST;
              objFnList[totalFragmentNum * nodeFragmentMap.size() + i * nodeFragmentMap.size()
                  + j + 1] = fragmentSize[i];
            }
          }
        }
        problem.setObjFn(objFnList);

        // 定义约束条件
        // 负载均衡约束
        for (int j = 0; j < nodeFragmentMap.size(); j++) {
          double[] loadConstraintList = new double[lpParamNum + 1];
          for (int i = 0; i < totalFragmentNum; i++) {
            loadConstraintList[i * nodeFragmentMap.size() + j + 1] = writeLoad[i];
            loadConstraintList[totalFragmentNum * nodeFragmentMap.size() + i * nodeFragmentMap
                .size()
                + j + 1] = readLoad[i];
          }
          problem.addConstraint(loadConstraintList, LpSolve.GE, minLoad);
          problem.addConstraint(loadConstraintList, LpSolve.LE, maxLoad);
        }

        // 每个分区必须分配在一个节点上
        for (int i = 0; i < totalFragmentNum; i++) {
          double[] writeFragmentConstraintList = new double[lpParamNum + 1];
          double[] readFragmentConstraintList = new double[lpParamNum + 1];
          for (int j = 0; j < nodeFragmentMap.size(); j++) {
            writeFragmentConstraintList[i * nodeFragmentMap.size() + j + 1] = 1;
            readFragmentConstraintList[totalFragmentNum * nodeFragmentMap.size()
                + i * nodeFragmentMap.size()
                + j + 1] = 1;
          }
          problem.addConstraint(writeFragmentConstraintList, LpSolve.EQ, 1);
          problem.addConstraint(readFragmentConstraintList, LpSolve.EQ, 1);
        }

        // 缩容条件限定
        if (toScaleInNodes.size() > 0) {
          for (int j = 0; j < nodeFragmentMap.size(); j++) {
            double[] loadConstraintList = new double[lpParamNum + 1];
            for (int i = 0; i < totalFragmentNum; i++) {
              loadConstraintList[i * nodeFragmentMap.size() + j + 1] = writeLoad[i];
              loadConstraintList[totalFragmentNum * nodeFragmentMap.size() + i * nodeFragmentMap
                  .size()
                  + j + 1] = readLoad[i];
            }
            problem.addConstraint(loadConstraintList, LpSolve.GE, minLoad);
            problem.addConstraint(loadConstraintList, LpSolve.LE, maxLoad);
          }
        }

        for (int nodeIndex : toScaleInNodeIndexList) {
          double[] scaleInNodeConstraintList = new double[lpParamNum + 1];
          for (int i = 0; i < totalFragmentNum; i++) {
            scaleInNodeConstraintList[i * nodeFragmentMap.size() + nodeIndex + 1] = 1;
            scaleInNodeConstraintList[totalFragmentNum * nodeFragmentMap.size()
                + i * nodeFragmentMap
                .size()
                + nodeIndex + 1] = 1;
          }
          problem.addConstraint(scaleInNodeConstraintList, LpSolve.EQ, 0);
        }

        res = problem.solve();
        logger.info("problem status: {}, loopTime: {}", res, loopTime);
        if (res != LpSolve.OPTIMAL) {
          // 退出优化模型
          problem.deleteLp();
        }
        loopTime++;
      }
      // 优化计算，输出最优解
      if (res == LpSolve.OPTIMAL) {
        double[] vars = new double[lpParamNum];
        problem.getVariables(vars);
        for (int i = 0; i < totalFragmentNum; i++) {
          for (int j = 0; j < nodeFragmentMap.size(); j++) {
            result[0][i][j] = (int) vars[i * nodeFragmentMap.size() + j];
            result[1][i][j] = (int) vars[totalFragmentNum * nodeFragmentMap.size()
                + i * nodeFragmentMap.size() + j];
          }
        }
      }
      // 退出优化模型
      problem.deleteLp();
    } catch (LpSolveException e) {
      logger.error("Concert exception caught: " + e);
    }

    return result;
  }
}
