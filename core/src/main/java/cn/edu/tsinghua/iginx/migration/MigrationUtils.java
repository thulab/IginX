package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.policy.dynamic.MigrationTask;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class MigrationUtils {

  public static void migrateData(long sourceNode, long targetNode, FragmentMeta fragmentMeta) {

  }

  public static void reshardFragment(long sourceNode, long targetNode, FragmentMeta fragmentMeta) {
  }

  public static void operateTaskAndRequest(long sourceNode, long targetNode,
      FragmentMeta fragmentMeta) {

  }

  public static List<Queue<MigrationTask>> createParallelQueueWithoutPriority(List<MigrationTask> migrationTasks) {
    return new ArrayList<>();
  }
}
