package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.List;
import java.util.Map;

public interface IPolicy {

  void notify(DataStatement statement);

  void init(IMetaManager iMetaManager);

  StorageEngineChangeHook getStorageEngineChangeHook();

  Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(
      DataStatement statement);

  Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnitsByStatement(
      DataStatement statement);

  Pair<FragmentMeta, StorageUnitMeta> generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
      String startPath, String endPath, long startTime, long endTime,
      List<Long> storageEngineList);

  void executeReshardAndMigration(Map<FragmentMeta, Long> fragmentMetaPointsMap,
      Map<Long, List<FragmentMeta>> nodeFragmentMap, Map<FragmentMeta, Long> fragmentWriteLoadMap,
      Map<FragmentMeta, Long> fragmentReadLoadMap);

  boolean isNeedReAllocate();

  void setNeedReAllocate(boolean needReAllocate);
}
