package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.mqtt.MQTTService;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorManager implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(MQTTService.class);

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
      Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
      Map<Long, List<FragmentMeta>> fragmentOfEachNode = loadFragmentOfEachNode(
          fragmentHeatWriteMap, fragmentHeatReadMap);
      List<Long> toScaleInNodes = new ArrayList<>();
      for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
        toScaleInNodes.add(storageEngineMeta.getId());
      }
      DefaultMetaManager.getInstance().executeReshard();
      //发起负载均衡
      policy.executeReshardAndMigration(fragmentMetaPointsMap, fragmentOfEachNode,
          fragmentHeatWriteMap, fragmentHeatReadMap, toScaleInNodes);
      metaManager.scaleInStorageEngines(storageEngineMetas);
      for (StorageEngineMeta meta : storageEngineMetas) {
        PhysicalEngineImpl.getInstance().getStorageManager().removeStorage(meta);
      }
      return true;
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
        metaManager.clearMonitors();
        Thread.sleep(interval * 1000L);
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
        Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
        Map<Long, List<FragmentMeta>> fragmentOfEachNode = loadFragmentOfEachNode(
            fragmentHeatWriteMap, fragmentHeatReadMap);

        long totalHeats = 0;
        long maxHeat = 0;
        long minHeat = Long.MAX_VALUE;
        for (Entry<Long, List<FragmentMeta>> fragmentOfEachNodeEntry : fragmentOfEachNode
            .entrySet()) {
          long heat = 0;
          List<FragmentMeta> fragmentMetas = fragmentOfEachNodeEntry.getValue();
          for (FragmentMeta fragmentMeta : fragmentMetas) {
            heat += fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
            heat += fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
          }
          logger.info("heat of node {} : {}", fragmentOfEachNodeEntry.getKey(), heat);

          totalHeats += heat;
          maxHeat = Math.max(maxHeat, heat);
          minHeat = Math.min(minHeat, heat);
        }
        double averageHeats = totalHeats * 1.0 / fragmentOfEachNode.size();
        // 判断负载均衡
        if (minHeat <= 0) {
          continue;
        }
        if (((1 - unbalanceThreshold) * averageHeats >= minHeat
            || (1 + unbalanceThreshold) * averageHeats <= maxHeat)) {
          DefaultMetaManager.getInstance().executeReshard();
          //发起负载均衡
          policy.executeReshardAndMigration(fragmentMetaPointsMap, fragmentOfEachNode,
              fragmentHeatWriteMap, fragmentHeatReadMap, new ArrayList<>());
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