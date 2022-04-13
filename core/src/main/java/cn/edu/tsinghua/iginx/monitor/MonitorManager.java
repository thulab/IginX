package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.mqtt.MQTTService;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

  @Override
  public void run() {
    while (true) {
      try {
        //清空节点信息
        metaManager.clearMonitors();
        Thread.sleep(interval * 1000L);
        metaManager.updateFragmentRequests(RequestsMonitor.getInstance().getWriteRequestsMap(),
            RequestsMonitor.getInstance()
                .getReadRequestsMap());
        metaManager.updateFragmentHeat(HotSpotMonitor.getInstance().getWriteHotspotMap(),
            HotSpotMonitor.getInstance().getReadHotspotMap());
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
        Map<Long, List<FragmentMeta>> fragmentOfEachNode = metaManager.loadFragmentOfEachNode();

        long totalHeats = 0;
        long maxHeat = 0;
        long minHeat = Long.MAX_VALUE;
        for (Entry<Long, List<FragmentMeta>> fragmentOfEachNodeEntry : fragmentOfEachNode
            .entrySet()) {
          long heat = 0;
          List<FragmentMeta> fragmentMetas = fragmentOfEachNodeEntry.getValue();
          for (FragmentMeta fragmentMeta : fragmentMetas) {
            heat += fragmentHeatWriteMap.get(fragmentMeta);
            heat += fragmentHeatReadMap.get(fragmentMeta);
          }
          totalHeats += heat;
          maxHeat = Math.max(maxHeat, heat);
          minHeat = Math.min(minHeat, heat);
        }
        double averageHeats = totalHeats * 1.0 / fragmentOfEachNode.size();
        // 判断负载均衡
        if ((1 - unbalanceThreshold) * averageHeats >= minHeat
            || (1 + unbalanceThreshold) * averageHeats <= maxHeat) {
          //发起负载均衡
          policy.executeReshardAndMigration(fragmentMetaPointsMap, fragmentOfEachNode,
              fragmentHeatWriteMap, fragmentHeatReadMap);
          //完成负载均衡
          DefaultMetaManager.getInstance().doneReshard();
        }
      } catch (Exception e) {
        logger.error("monitor manager error ", e);
      }
    }
  }
}