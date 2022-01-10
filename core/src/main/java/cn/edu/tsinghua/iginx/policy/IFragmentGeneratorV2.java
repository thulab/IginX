package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.policy.sampler.Sampler;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.List;

public interface IFragmentGeneratorV2 {

    Sampler getSampler();

    Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(List<String> paths, TimeInterval timeInterval);

    Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(List<String> prefixList, long startTime);

    Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(long startTime);

    void init(IMetaManager iMetaManager);

}
