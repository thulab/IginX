package cn.edu.tsinghua.iginx.monitor;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HotSpotMonitor implements IMonitor {

  private final boolean isEnableMonitor = ConfigDescriptor.getInstance().getConfig().isEnableMonitor();
  private final Map<FragmentMeta, Long> writeHotspotMap = new ConcurrentHashMap<>(); // 数据分区->写入总请求时间
  private final Map<FragmentMeta, Long> readHotspotMap = new ConcurrentHashMap<>(); // 数据分区->查询总请求时间
  private final Map<Long, Long> taskIdStartTimeMap = new ConcurrentHashMap<>(); // 任务ID->任务开始时间
  private static final HotSpotMonitor instance = new HotSpotMonitor();

  public static HotSpotMonitor getInstance() {
    return instance;
  }

  public Map<FragmentMeta, Long> getWriteHotspotMap() {
    return writeHotspotMap;
  }

  public Map<FragmentMeta, Long> getReadHotspotMap() {
    return readHotspotMap;
  }

  public void recordBefore(long taskId) {
    if (isEnableMonitor) {
      taskIdStartTimeMap.put(taskId, System.currentTimeMillis());
    }
  }

  public void recordAfter(long taskId, FragmentMeta fragmentMeta, OperatorType operatorType) {
    if (isEnableMonitor && taskIdStartTimeMap.containsKey(taskId)) {
      long duration = System.currentTimeMillis() - taskIdStartTimeMap.get(taskId);
      if (operatorType == OperatorType.Project) {
        long prevDuration = readHotspotMap.getOrDefault(fragmentMeta, 0L);
        readHotspotMap.put(fragmentMeta, prevDuration + duration);
      } else if (operatorType == OperatorType.Insert) {
        long prevDuration = writeHotspotMap.getOrDefault(fragmentMeta, 0L);
        writeHotspotMap.put(fragmentMeta, prevDuration + duration);
      }
      taskIdStartTimeMap.remove(taskId);
    }
  }

  @Override
  public void clear() {
    writeHotspotMap.clear();
    readHotspotMap.clear();
    taskIdStartTimeMap.clear();
  }
}