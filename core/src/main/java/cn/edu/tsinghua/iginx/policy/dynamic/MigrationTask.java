package cn.edu.tsinghua.iginx.policy.dynamic;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import lombok.Data;

@Data
public class MigrationTask {

  private FragmentMeta fragmentMeta;
  private double load;
  private long size;
  private String sourceStorageUnitId;
  private String targetStorageUnitId;
  private MigrationType migrationType;

  public MigrationTask(FragmentMeta fragmentMeta, double load, long size,
      String sourceStorageUnitId, String targetStorageUnitId,
      MigrationType migrationType) {
    this.fragmentMeta = fragmentMeta;
    this.load = load;
    this.size = size;
    this.sourceStorageUnitId = sourceStorageUnitId;
    this.targetStorageUnitId = targetStorageUnitId;
    this.migrationType = migrationType;
  }
}
