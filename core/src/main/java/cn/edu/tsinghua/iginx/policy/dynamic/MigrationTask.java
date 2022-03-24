package cn.edu.tsinghua.iginx.policy.dynamic;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import lombok.Data;

@Data
public class MigrationTask {

  private FragmentMeta fragmentMeta;
  private double load;
  private long size;
  private long sourceNodeId;
  private long targetNodeId;
  private MigrationType migrationType;
}
