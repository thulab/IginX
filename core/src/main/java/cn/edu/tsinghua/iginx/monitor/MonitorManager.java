package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.mqtt.MQTTService;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonitorManager implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(MQTTService.class);

  private static final int interval = ConfigDescriptor.getInstance().getConfig()
      .getLoadBalanceCheckInterval();

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
        metaManager.updateNodeLoadScore(NodeResourceMonitor.getInstance().getScore());
        Map<Long, Double> nodeLoadScores = metaManager.loadNodeLoadScores();
        // 判断负载均衡
        boolean isUnbalance = true;
        if (isUnbalance) {
          NodePerformanceMonitor.getInstance().start();
          HotSpotMonitor.getInstance().start();
          //通知各个节点开始收集请求级信息
          //等待收集完成
          //集中信息
          //发起负载均衡
        }
      } catch (Exception e) {
        logger.error("monitor manager error ", e);
      }
    }
  }
}