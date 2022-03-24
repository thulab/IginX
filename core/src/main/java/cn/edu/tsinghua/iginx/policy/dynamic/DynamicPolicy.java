package cn.edu.tsinghua.iginx.policy.dynamic;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.monitor.NodeResource;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamicPolicy implements IPolicy {

  @Override
  public void notify(DataStatement statement) {

  }

  @Override
  public void init(IMetaManager iMetaManager) {

  }

  @Override
  public StorageEngineChangeHook getStorageEngineChangeHook() {
    return null;
  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(
      DataStatement statement) {
    return null;
  }

  @Override
  public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(
      DataStatement statement) {
    return null;
  }

  @Override
  public List<MigrationTask> generateReshardFinalStatus(
      Map<Long, NodeResource> nodeRestResourcesMap, Map<Long, NodeResource> nodeUsedResourcesMap,
      Map<Long, List<FragmentMeta>> nodeFragmentMap, Map<FragmentMeta, Long> fragmentWriteLoadMap,
      Map<FragmentMeta, Long> fragmentReadLoadMap) {
    List<NodeResource> usedResourcesListForCalculation = new ArrayList<>();
    List<Long> writeLoadListForCalculation = new ArrayList<>();
    List<Long> readLoadListForCalculation = new ArrayList<>();
    int countNeededForCalculation = 2;
    for (long key : nodeUsedResourcesMap.keySet()) {
      if (countNeededForCalculation <= 0) {
        break;
      }
      usedResourcesListForCalculation.add(nodeUsedResourcesMap.get(key));
      List<FragmentMeta> fragments = nodeFragmentMap.get(key);
      long writeLoadOfNode = 0;
      long readLoadOfNode = 0;
      for (FragmentMeta fragmentMeta : fragments) {
        writeLoadOfNode += fragmentWriteLoadMap.get(fragmentMeta);
        readLoadOfNode += fragmentReadLoadMap.get(fragmentMeta);
      }
      writeLoadListForCalculation.add(writeLoadOfNode);
      readLoadListForCalculation.add(readLoadOfNode);
      countNeededForCalculation--;
    }
    double[] params = calculateWriteAndReadResourceUsed(usedResourcesListForCalculation,
        writeLoadListForCalculation, readLoadListForCalculation);
    //TODO
    return new ArrayList<>();
  }

  private double[] calculateWriteAndReadResourceUsed(List<NodeResource> usedResourcesList,
      List<Long> writeLoadList, List<Long> readLoadList) {
    double[] results = new double[8];
    //TODO
    //增广矩阵法求解
    return results;
  }

  @Override
  public boolean isNeedReAllocate() {
    return false;
  }

  @Override
  public void setNeedReAllocate(boolean needReAllocate) {

  }
}
