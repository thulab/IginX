package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.mqtt.MQTTService;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorManager implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(MQTTService.class);

  private static final int interval = ConfigDescriptor.getInstance().getConfig()
      .getLoadBalanceCheckInterval();
  private static final int requestFilterMonitorPeriod = ConfigDescriptor.getInstance().getConfig()
      .getRequestFilterMonitorPeriod();
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
    NodeResourceMonitor.getInstance().start();
    while (true) {
      try {
        Thread.sleep(interval);
        metaManager.updateNodeLoadScore(NodeResourceMonitor.getInstance().getNodeResource());
        Map<Long, NodeResource> nodeResourceMap = metaManager.loadNodeLoadScores();
        double totalScores = 0;
        double maxScore = 0;
        double minScore = Long.MAX_VALUE;
        for (Entry<Long, NodeResource> nodeResourceEntry : nodeResourceMap.entrySet()) {
          double score = nodeResourceEntry.getValue().getScore();
          totalScores += score;
          maxScore = Math.max(maxScore, score);
          minScore = Math.min(minScore, score);
        }
        double averageScore = totalScores / nodeResourceMap.size();
        // 判断负载均衡
        if ((1 - unbalanceThreshold) * averageScore >= minScore
            || (1 + unbalanceThreshold) * averageScore <= maxScore) {
          //通知各个节点开始收集请求级信息
          metaManager.startMonitors();
          //等待收集完成
          while (!metaManager.isAllMonitorsCompleteCollection()) {
            Thread.sleep(1000);
          }
          //集中信息（初版主要是统计分区热度）
          Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> fragmentHeatPair = metaManager
              .loadFragmentHeat();
          Map<Long, NodeResource> currNodeResourceMap = metaManager.loadNodeLoadScores();
          Map<String, List<FragmentMeta>> fragmentMetaListMap = metaManager
              .loadFragmentOfEachNode();
          //发起负载均衡
          policy.executeReshardAndMigration(currNodeResourceMap, fragmentMetaListMap,
              fragmentHeatPair.getK(), fragmentHeatPair.getV());
        }
      } catch (Exception e) {
        logger.error("monitor manager error ", e);
      }
    }
  }

  public void monitorNodePerformanceAndHotSpot() {
    try {
      NodePerformanceMonitor.getInstance().start();
      HotSpotMonitor.getInstance().start();
      Thread.sleep(requestFilterMonitorPeriod);
      NodePerformanceMonitor.getInstance().stop();
      HotSpotMonitor.getInstance().stop();
      metaManager.updateNodePerformance(NodePerformanceMonitor.getInstance().getWriteLatency(),
          NodePerformanceMonitor.getInstance().getReadLatency());
      metaManager.updateFragmentHeat(HotSpotMonitor.getInstance().getWriteHotspotMap(),
          HotSpotMonitor.getInstance().getReadHotspotMap());
    } catch (Exception e) {
      logger.error("monitor node performance and hot spot error ", e);
    }
  }
}