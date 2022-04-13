package cn.edu.tsinghua.iginx.policy.dynamic;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.migration.MigrationManager;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.Utils;
import cn.edu.tsinghua.iginx.policy.simple.FragmentCreator;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import ilog.concert.IloException;
import ilog.concert.IloIntVar;
import ilog.concert.IloLinearNumExpr;
import ilog.concert.IloNumExpr;
import ilog.cplex.IloCplex;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicPolicy implements IPolicy {

  protected AtomicBoolean needReAllocate = new AtomicBoolean(false);
  private IMetaManager iMetaManager;
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(DynamicPolicy.class);
  private static final double unbalanceThreshold = config.getUnbalanceThreshold();

  @Override
  public void notify(DataStatement statement) {
    if (statement.getType() == StatementType.INSERT) {
      iMetaManager.saveTimeSeriesData((InsertStatement) statement);
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
      if (before == null && after != null && after.getCreatedBy() == iMetaManager.getIginxId()
          && after.isLastOfBatch()) {
        needReAllocate.set(true);
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

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnitsByStatement(
      DataStatement statement) {
    logger.debug("dynamic policy do not have to reallocate fragments by one statement");
    return new Pair<>(new ArrayList<>(), new ArrayList<>());
  }

  public boolean isNeedReAllocate() {
    return needReAllocate.getAndSet(false);
  }

  public void setNeedReAllocate(boolean needReAllocate) {
    this.needReAllocate.set(needReAllocate);
  }

  public boolean checkSuccess(Map<String, Double> timeseriesData) {
    Map<TimeSeriesInterval, FragmentMeta> latestFragments = iMetaManager.getLatestFragmentMap();
    Map<TimeSeriesInterval, Double> fragmentValue = latestFragments.keySet().stream().collect(
        Collectors.toMap(Function.identity(), e1 -> 0.0, (e1, e2) -> e1)
    );
    timeseriesData.forEach((key, value) -> {
      for (TimeSeriesInterval timeSeriesInterval : fragmentValue.keySet()) {
        if (timeSeriesInterval.isContain(key)) {
          Double tmp = fragmentValue.get(timeSeriesInterval);
          fragmentValue.put(timeSeriesInterval, value + tmp);
        }
      }
    });
    List<Double> value = fragmentValue.values().stream().sorted().collect(Collectors.toList());
    int num = 0;
    for (Double v : value) {
      logger.info("fragment value num : {}, value : {}", num++, v);
    }
    if (value.size() > 0) {
      return !(value.get(new Double(Math.ceil(value.size() - 1) * 0.9).intValue())
          > config.getStorageGroupValueLimit() * 3);
    } else {
      return true;
    }
  }

  @Override
  public void executeReshardAndMigration(
      Map<FragmentMeta, Long> fragmentMetaPointsMap,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap) {
    // 平均资源占用
    double totalHeat = 0;
    for (long heat : fragmentWriteLoadMap.values()) {
      totalHeat += heat;
    }
    for (long heat : fragmentReadLoadMap.values()) {
      totalHeat += heat;
    }
    int totalFragmentNum = 0;
    for (List<FragmentMeta> fragmentMetas : nodeFragmentMap.values()) {
      totalFragmentNum += fragmentMetas.size();
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
    int[][][] migrationResults = calculateMigrationFinalStatus(totalHeat / nodeFragmentMap.size(),
        fragmentMetaPointsMap, nodeFragmentMap, fragmentWriteLoadMap, fragmentReadLoadMap,
        totalFragmentNum, m);

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
        if (m[i][j] == 1) {
          // 写要迁移
          if (migrationResults[0][i][j] == 0) {
            int targetIndex;
            for (targetIndex = 0; targetIndex < allNodes.length; targetIndex++) {
              if (migrationResults[0][i][targetIndex] == 1) {
                break;
              }
            }
            migrationTasks.add(new MigrationTask(fragmentMeta,
                fragmentWriteLoadMap.get(fragmentMeta),
                fragmentMetaPointsMap.get(fragmentMeta), allNodes[j], allNodes[targetIndex],
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
            migrationTasks.add(new MigrationTask(fragmentMeta,
                fragmentReadLoadMap.get(fragmentMeta),
                fragmentMetaPointsMap.get(fragmentMeta), allNodes[j], allNodes[targetIndex],
                MigrationType.QUERY));
          }
        }
      }
    }

    // 执行迁移
    MigrationManager.getInstance().getMigration()
        .migrate(migrationTasks, nodeFragmentMap, fragmentWriteLoadMap, fragmentReadLoadMap);
  }

  private int[][][] calculateMigrationFinalStatus(double averageScore,
      Map<FragmentMeta, Long> fragmentMetaPointsMap,
      Map<Long, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap,
      int totalFragmentNum, int[][] m) {
    int[][][] result = new int[2][totalFragmentNum][nodeFragmentMap.size()];

    try {
      // 声明cplex优化模型
      IloCplex model = new IloCplex();

      // 定义两个二维优化变量
      IloIntVar[][] xr = new IloIntVar[totalFragmentNum][nodeFragmentMap.size()];
      for (int i = 0; i < totalFragmentNum; i++) {
        for (int j = 0; j < nodeFragmentMap.size(); j++) {
          xr[i][j] = model.intVar(0, 1, "xr[" + i + "," + j + "]");
        }
      }
      IloIntVar[][] xw = new IloIntVar[totalFragmentNum][nodeFragmentMap.size()];
      for (int i = 0; i < totalFragmentNum; i++) {
        for (int j = 0; j < nodeFragmentMap.size(); j++) {
          xw[i][j] = model.intVar(0, 1, "xw[" + i + "," + j + "]");
        }
      }

      //分区大小函数
      long[] fragmentSize = new long[totalFragmentNum];
      long[] writeLoad = new long[totalFragmentNum];
      long[] readLoad = new long[totalFragmentNum];
      int currIndex = 0;
      for (List<FragmentMeta> fragmentMetas : nodeFragmentMap.values()) {
        for (FragmentMeta fragmentMeta : fragmentMetas) {
          fragmentSize[currIndex] = fragmentMetaPointsMap.get(fragmentMeta);
          writeLoad[currIndex] = fragmentWriteLoadMap.get(fragmentMeta);
          readLoad[currIndex] = fragmentReadLoadMap.get(fragmentMeta);
          currIndex++;
        }
      }

      // 定义目标函数
      IloLinearNumExpr objExpr = model.linearNumExpr();
      for (int i = 0; i < totalFragmentNum; i++) {
        for (int j = 0; j < nodeFragmentMap.size(); j++) {
          IloIntVar prodResult = (IloIntVar) model.prod(xr[i][j], 1 - m[i][j]);
          objExpr.addTerm(prodResult, fragmentSize[i]);
        }
      }
      model.addMinimize(objExpr);

      // 定义约束条件
      // 负载均衡约束
      for (int j = 0; j < nodeFragmentMap.size(); j++) {
        IloNumExpr[] currLoads = new IloNumExpr[totalFragmentNum];
        for (int i = 0; i < totalFragmentNum; i++) {
          IloNumExpr wl = model.prod(xw[i][j], writeLoad[i]);
          IloNumExpr rl = model.prod(xr[i][j], readLoad[i]);
          currLoads[i] = model.sum(new IloNumExpr[]{wl, rl});
        }
        model.addGe(model.sum(currLoads), averageScore * (1 - unbalanceThreshold));
        model.addLe(model.sum(currLoads), averageScore * (1 + unbalanceThreshold));
      }
      // 每个分区必须分配在一个节点上
      for (int i = 0; i < totalFragmentNum; i++) {
        model.addEq(model.sum(xr[i]), 1);
        model.addEq(model.sum(xw[i]), 1);
      }

      // 优化计算，输出最优解
      if (model.solve()) {
        for (int i = 0; i < totalFragmentNum; i++) {
          for (int j = 0; j < nodeFragmentMap.size(); j++) {
            result[0][i][j] = (int) model.getValue(xw[i][j]);
          }
        }
        for (int i = 0; i < totalFragmentNum; i++) {
          for (int j = 0; j < nodeFragmentMap.size(); j++) {
            result[1][i][j] = (int) model.getValue(xr[i][j]);
          }
        }
      }
      // 退出优化模型
      model.end();
    } catch (IloException e) {
      System.err.println("Concert exception caught: " + e);
    }
    return result;
  }
}
