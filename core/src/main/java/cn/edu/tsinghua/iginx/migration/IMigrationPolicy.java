package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.monitor.NodeResource;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import java.util.List;
import java.util.Map;

public interface IMigrationPolicy {

  void migrate(List<MigrationTask> migrationTasks, Map<Long, NodeResource> nodeRestResourcesMap,
      double[] costParams);

  void interrupt();

  void recover();
}
