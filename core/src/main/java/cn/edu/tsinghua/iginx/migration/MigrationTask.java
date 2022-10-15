package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import lombok.Data;

@Data
public class MigrationTask {

  public static final String SEPARATOR = "-";
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

  @Override
  public String toString() {
    return fragmentMeta.getTimeInterval().getStartTime()
        + SEPARATOR
        + fragmentMeta.getTimeInterval().getEndTime()
        + SEPARATOR
        + fragmentMeta.getTsInterval().getStartTimeSeries()
        + SEPARATOR
        + fragmentMeta.getTsInterval().getEndTimeSeries()
        + SEPARATOR
        + fragmentMeta.getMasterStorageUnitId()
        + SEPARATOR
        + load
        + SEPARATOR
        + size
        + SEPARATOR
        + sourceStorageId
        + SEPARATOR
        + targetStorageId
        + SEPARATOR
        + migrationType;
  }

  public static MigrationTask fromString(String input) {
    String[] tuples = input.split(SEPARATOR);
    return new MigrationTask(new FragmentMeta(tuples[2], tuples[3], Long.parseLong(tuples[0]),
        Long.parseLong(tuples[1]), tuples[4]), Long.parseLong(tuples[5]), Long.parseLong(tuples[6]),
        Long.parseLong(tuples[7]), Long.parseLong(tuples[8]), MigrationType.valueOf(tuples[9]));
  }
}
