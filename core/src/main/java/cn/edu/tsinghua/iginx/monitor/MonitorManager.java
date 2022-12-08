package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.storage.execute.StoragePhysicalTaskExecutor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;

public class MonitorManager implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(MonitorManager.class);

  private static final int interval = ConfigDescriptor.getInstance().getConfig()
      .getLoadBalanceCheckInterval();
  private static final double unbalanceThreshold = ConfigDescriptor.getInstance().getConfig()
      .getUnbalanceThreshold();
  private final IPolicy policy = PolicyManager.getInstance()
      .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName());

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();
  private static MonitorManager INSTANCE;

  public static MonitorManager getInstance() {
    if (INSTANCE == null) {
      synchronized (MonitorManager.class) {
        if (INSTANCE == null) {
          INSTANCE = new MonitorManager();
        }
      }
    }
    return INSTANCE;
  }

  public boolean scaleInStorageEngines(List<StorageEngineMeta> storageEngineMetas) {
    try {
      //如果上一轮负载均衡还在继续 必须要等待其结束
      while (DefaultMetaManager.getInstance().isResharding()) {
        Thread.sleep(1000);
      }
      //发起负载均衡判断
      DefaultMetaManager.getInstance().executeReshardJudging();
      metaManager.updateFragmentRequests(RequestsMonitor.getInstance().getWriteRequestsMap(),
          RequestsMonitor.getInstance()
              .getReadRequestsMap());
      metaManager.submitMaxActiveEndTime();
      Map<FragmentMeta, Long> writeHotspotMap = HotSpotMonitor.getInstance().getWriteHotspotMap();
      Map<FragmentMeta, Long> readHotspotMap = HotSpotMonitor.getInstance().getReadHotspotMap();
      metaManager.updateFragmentHeat(writeHotspotMap, readHotspotMap);
      //等待收集完成
      while (!metaManager.isAllMonitorsCompleteCollection()) {
        Thread.sleep(1000);
      }
      //集中信息（初版主要是统计分区热度）
      Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> fragmentHeatPair = metaManager
          .loadFragmentHeat();
      Map<FragmentMeta, Long> fragmentHeatWriteMap = fragmentHeatPair.getK();
      Map<FragmentMeta, Long> fragmentHeatReadMap = fragmentHeatPair.getV();
      if (fragmentHeatWriteMap == null) {
        fragmentHeatWriteMap = new HashMap<>();
      }
      if (fragmentHeatReadMap == null) {
        fragmentHeatReadMap = new HashMap<>();
      }
      Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
      Map<Long, List<FragmentMeta>> fragmentOfEachNode = loadFragmentOfEachNode(
          fragmentHeatWriteMap, fragmentHeatReadMap);
      List<Long> toScaleInNodes = new ArrayList<>();
      for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
        toScaleInNodes.add(storageEngineMeta.getId());
      }
      if (DefaultMetaManager.getInstance().executeReshard()) {
        //发起负载均衡
        policy.executeReshardAndMigration(fragmentMetaPointsMap, fragmentOfEachNode,
            fragmentHeatWriteMap, fragmentHeatReadMap, toScaleInNodes);
        metaManager.scaleInStorageEngines(storageEngineMetas);
        for (StorageEngineMeta meta : storageEngineMetas) {
          PhysicalEngineImpl.getInstance().getStorageManager().removeStorage(meta);
        }
        return true;
      }
      return false;
    } catch (Exception e) {
      logger.error("execute scale-in reshard failed :", e);
      return false;
    } finally {
      //完成一轮负载均衡
      DefaultMetaManager.getInstance().doneReshard();
    }
  }

  @Override
  public void run() {
    while (true) {
      try {
        //清空节点信息
        logger.error("start to clear monitors");
        metaManager.clearMonitors();
        logger.error("end clear monitors");
        Thread.sleep(interval * 1000L);
        logger.error("allRequests = {}", StoragePhysicalTaskExecutor.getInstance().allRequests);
        logger.error("submittedRequests = {}", StoragePhysicalTaskExecutor.getInstance().submittedRequests);
        logger.error("completedRequests = {}", StoragePhysicalTaskExecutor.getInstance().completedRequests);

        //发起负载均衡判断
        DefaultMetaManager.getInstance().executeReshardJudging();
        metaManager.updateFragmentRequests(RequestsMonitor.getInstance().getWriteRequestsMap(),
            RequestsMonitor.getInstance()
                .getReadRequestsMap());

        long totalWriteRequests = 0;
        logger.error("start to print all requests of each fragments");
        Map<FragmentMeta, Long> writeRequestsMap = RequestsMonitor.getInstance()
            .getWriteRequestsMap();
        for (Entry<FragmentMeta, Long> requestsOfEachFragment : writeRequestsMap
            .entrySet()) {
          totalWriteRequests += requestsOfEachFragment.getValue();
          logger.error("fragment requests: {} = {}", requestsOfEachFragment.getKey().toString(),
              requestsOfEachFragment.getValue());
        }
        logger.error("end print all requests of each fragments");
        logger.error("total write requests: {}", totalWriteRequests);

        metaManager.submitMaxActiveEndTime();
        Map<FragmentMeta, Long> writeHotspotMap = HotSpotMonitor.getInstance().getWriteHotspotMap();
        Map<FragmentMeta, Long> readHotspotMap = HotSpotMonitor.getInstance().getReadHotspotMap();
        logger.error("writeHotspotMap = {}", writeHotspotMap);
        logger.error("readHotspotMap = {}", readHotspotMap);
        metaManager.updateFragmentHeat(writeHotspotMap, readHotspotMap);
        //等待收集完成
//        int waitTime = 0;
//        while (!metaManager.isAllMonitorsCompleteCollection()) {
//          Thread.sleep(100);
//          logger.error("waiting for complete");
//          waitTime++;
//          if (waitTime > 10) {
//            logger.error("monitor collection wait time more than {} ms", waitTime * 100);
//            break;
//          }
//        }
        Thread.sleep(1000);
        logger.error("start to load fragments heat");
        //集中信息（初版主要是统计分区热度）
        Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> fragmentHeatPair = metaManager
            .loadFragmentHeat();
        Map<FragmentMeta, Long> fragmentHeatWriteMap = fragmentHeatPair.getK();
        Map<FragmentMeta, Long> fragmentHeatReadMap = fragmentHeatPair.getV();
        if (fragmentHeatWriteMap == null) {
          fragmentHeatWriteMap = new HashMap<>();
        }
        if (fragmentHeatReadMap == null) {
          fragmentHeatReadMap = new HashMap<>();
        }
        logger.error("start to load fragments points");
        Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
        logger.error("start to load fragment of each node");
        Map<Long, List<FragmentMeta>> fragmentOfEachNode = loadFragmentOfEachNode(
            fragmentHeatWriteMap, fragmentHeatReadMap);

        long totalHeats = 0;
        long maxHeat = 0;
        long minHeat = Long.MAX_VALUE;
        logger.error("start to print all fragments of each node");
        for (Entry<Long, List<FragmentMeta>> fragmentOfEachNodeEntry : fragmentOfEachNode
            .entrySet()) {
          long heat = 0;
          long requests = 0;
          List<FragmentMeta> fragmentMetas = fragmentOfEachNodeEntry.getValue();
          for (FragmentMeta fragmentMeta : fragmentMetas) {
            logger.error("fragment: {}", fragmentMeta.toString());
            logger.error("fragment heat write: {} = {}", fragmentMeta,
                fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L));
            heat += fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
            logger.error("fragment heat read: {} = {}", fragmentMeta,
                fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L));
            heat += fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
            requests += writeRequestsMap.getOrDefault(fragmentMeta, 0L);
          }
          logger.error("heat of node {} : {}", fragmentOfEachNodeEntry.getKey(), heat);
          logger.error("requests of node {} : {}", fragmentOfEachNodeEntry.getKey(), requests);

          totalHeats += heat;
          maxHeat = Math.max(maxHeat, heat);
          minHeat = Math.min(minHeat, heat);
        }
        logger.error("end print all fragments of each node");
        double averageHeats = totalHeats * 1.0 / fragmentOfEachNode.size();

        if (((1 - unbalanceThreshold) * averageHeats >= minHeat
            || (1 + unbalanceThreshold) * averageHeats <= maxHeat)) {
          logger.error("start to execute reshard");
          if (DefaultMetaManager.getInstance().executeReshard()) {
            //发起负载均衡
            policy.executeReshardAndMigration(fragmentMetaPointsMap, fragmentOfEachNode,
                fragmentHeatWriteMap, fragmentHeatReadMap, new ArrayList<>());
          } else {
            logger.error("execute reshard failed");
          }
        }
      } catch (Exception e) {
        logger.error("monitor manager error ", e);
      } finally {
        //完成一轮负载均衡
        DefaultMetaManager.getInstance().doneReshard();
      }
    }
  }

  private Map<Long, List<FragmentMeta>> loadFragmentOfEachNode(
      Map<FragmentMeta, Long> fragmentHeatWriteMap, Map<FragmentMeta, Long> fragmentHeatReadMap) {
    Set<FragmentMeta> fragmentMetaSet = new HashSet<>();
    Map<Long, List<FragmentMeta>> result = new HashMap<>();
    fragmentMetaSet.addAll(fragmentHeatWriteMap.keySet());
    fragmentMetaSet.addAll(fragmentHeatReadMap.keySet());

    for (FragmentMeta fragmentMeta : fragmentMetaSet) {
      fragmentMetaSet.add(fragmentMeta);
      List<FragmentMeta> fragmentMetas = result
          .computeIfAbsent(fragmentMeta.getMasterStorageUnit().getStorageEngineId(),
              k -> new ArrayList<>());
      fragmentMetas.add(fragmentMeta);
    }

    List<StorageEngineMeta> storageEngineMetas = metaManager.getStorageEngineList();
    Set<Long> storageIds = result.keySet();
    for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
      if (!storageIds.contains(storageEngineMeta.getId())) {
        result.put(storageEngineMeta.getId(), new ArrayList<>());
      }
    }
    return result;
  }
}