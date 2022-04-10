package cn.edu.tsinghua.iginx.policy.dynamic;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import lombok.Data;

@Data
public class MigrationTask {

  private static final long WRITE_LOAD_MIGRATION_COST = 10;

  private FragmentMeta fragmentMeta;
  private long load;
  private long size;
  private String sourceStorageUnitId;
  private String targetStorageUnitId;
  private MigrationType migrationType;

  public MigrationTask(FragmentMeta fragmentMeta, long load, long size,
      String sourceStorageUnitId, String targetStorageUnitId,
      MigrationType migrationType) {
    this.fragmentMeta = fragmentMeta;
    this.load = load;
    this.size = size;
    this.sourceStorageUnitId = sourceStorageUnitId;
    this.targetStorageUnitId = targetStorageUnitId;
    this.migrationType = migrationType;
  }

  public double getPriorityScore() {
    switch (migrationType) {
      case WRITE:
        return load * 1.0 / WRITE_LOAD_MIGRATION_COST;
      case QUERY:
      default:
        return load * 1.0 / size;
    }
  }

  public long getMigrationSize() {
    switch (migrationType) {
      case WRITE:
        return WRITE_LOAD_MIGRATION_COST;
      case QUERY:
      default:
        return size;
    }
  }
}
