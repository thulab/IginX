package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.List;

public interface IPolicy {

    void notify(DataStatement statement);

    void init(IMetaManager iMetaManager);

    StorageEngineChangeHook getStorageEngineChangeHook();

    Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(DataStatement statement);

    Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(DataStatement statement);

    Pair<FragmentMeta, StorageUnitMeta> generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(
        String startPath, String endPath, long startTime, long endTime,
        List<Long> storageEngineList);

    boolean isNeedReAllocate();

    void setNeedReAllocate(boolean needReAllocate);
}
