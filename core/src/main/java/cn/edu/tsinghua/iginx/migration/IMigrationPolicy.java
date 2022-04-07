package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import java.util.List;
import java.util.Map;

public interface IMigrationPolicy {

  void migrate(List<MigrationTask> migrationTasks, Map<String, List<FragmentMeta>> nodeFragmentMap,
      Map<FragmentMeta, Long> fragmentWriteLoadMap, Map<FragmentMeta, Long> fragmentReadLoadMap);

  void interrupt();

  void recover();
}
