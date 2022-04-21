package cn.edu.tsinghua.iginx.policy.dynamic;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import lombok.Data;

@Data
public class MigrationTask {

  public static final long RESHARD_MIGRATION_COST = 10;

  private FragmentMeta fragmentMeta;
  private long load;
  private long size;
  private Long sourceStorageId;
  private Long targetStorageId;
  private MigrationType migrationType;

  public MigrationTask(FragmentMeta fragmentMeta, long load, long size,
      Long sourceStorageId, Long targetStorageId,
      MigrationType migrationType) {
    this.fragmentMeta = fragmentMeta;
    this.load = load;
    this.size = size;
    this.sourceStorageId = sourceStorageId;
    this.targetStorageId = targetStorageId;
    this.migrationType = migrationType;
  }

  public double getPriorityScore() {
    switch (migrationType) {
      case WRITE:
        return load * 1.0 / RESHARD_MIGRATION_COST;
      case QUERY:
      default:
        return load * 1.0 / size;
    }
  }

  public long getMigrationSize() {
    switch (migrationType) {
      case WRITE:
        return RESHARD_MIGRATION_COST;
      case QUERY:
      default:
        return size;
    }
  }
}
