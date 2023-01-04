package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.compaction.CompactionManager;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MonitorManager implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(MonitorManager.class);

  private static final int interval = ConfigDescriptor.getInstance().getConfig()
      .getLoadBalanceCheckInterval();

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();
  private final CompactionManager compactionManager = CompactionManager.getInstance();
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
        logger.info("start to clear monitors");
        compactionManager.clearFragment();
        metaManager.clearMonitors();
        logger.error("end clear monitors");
        Thread.sleep(interval * 1000L);

        //上传本地统计数据
        metaManager.updateFragmentRequests(RequestsMonitor.getInstance().getWriteRequestsMap(),
            RequestsMonitor.getInstance()
                .getReadRequestsMap());
        metaManager.submitMaxActiveEndTime();
        Map<FragmentMeta, Long> writeHotspotMap = HotSpotMonitor.getInstance().getWriteHotspotMap();
        Map<FragmentMeta, Long> readHotspotMap = HotSpotMonitor.getInstance().getReadHotspotMap();
        metaManager.updateFragmentHeat(writeHotspotMap, readHotspotMap);
      } catch (Exception e) {
        logger.error("monitor manager error ", e);
      }
    }
  }
}