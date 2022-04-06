package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class MigrationUtils {

  public static void migrateData(String sourceStorageUnitId, String targetStorageUnitId, FragmentMeta fragmentMeta) {

  }

  public static void reshardFragment(String sourceStorageUnitId, String targetStorageUnitId, FragmentMeta fragmentMeta) {
  }

  public static void operateTaskAndRequest(String sourceStorageUnitId, String targetStorageUnitId,
      FragmentMeta fragmentMeta) {

  }

  public static List<Queue<MigrationTask>> createParallelQueueWithoutPriority(List<MigrationTask> migrationTasks) {
    return new ArrayList<>();
  }
}
