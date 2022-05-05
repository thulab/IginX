package cn.edu.tsinghua.iginx.migration.recover;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import lombok.Data;

@Data
public class MigrationExecuteTask {

  public static final String SEPARATOR = "-";
  public static final long RESHARD_MIGRATION_COST = 10;

  private FragmentMeta fragmentMeta;
  private String masterStorageUnitId;
  private Long sourceStorageId;
  private Long targetStorageId;
  private MigrationExecuteType migrationExecuteType;

  public MigrationExecuteTask(FragmentMeta fragmentMeta,
      String masterStorageUnitId, Long sourceStorageId, Long targetStorageId,
      MigrationExecuteType migrationExecuteType) {
    this.fragmentMeta = fragmentMeta;
    this.masterStorageUnitId = masterStorageUnitId;
    this.sourceStorageId = sourceStorageId;
    this.targetStorageId = targetStorageId;
    this.migrationExecuteType = migrationExecuteType;
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
        + masterStorageUnitId
        + SEPARATOR
        + sourceStorageId
        + SEPARATOR
        + targetStorageId
        + SEPARATOR
        + migrationExecuteType;
  }

  public static MigrationExecuteTask fromString(String input) {
    String[] tuples = input.split(SEPARATOR);
    return new MigrationExecuteTask(
        new FragmentMeta(tuples[2], tuples[3], Long.parseLong(tuples[0]),
            Long.parseLong(tuples[1]), tuples[4]), tuples[4], Long.parseLong(tuples[5]),
        Long.parseLong(tuples[6]), MigrationExecuteType.valueOf(tuples[7]));
  }
}
