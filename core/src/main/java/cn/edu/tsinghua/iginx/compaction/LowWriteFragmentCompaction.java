package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LowWriteFragmentCompaction extends Compaction {

    private static final Logger logger = LoggerFactory.getLogger(LowWriteFragmentCompaction.class);
    private static final long fragmentCompactionWriteThreshold = ConfigDescriptor.getInstance().getConfig().getFragmentCompactionWriteThreshold();
    private static final double fragmentCompactionReadRatioThreshold = ConfigDescriptor.getInstance().getConfig().getFragmentCompactionReadRatioThreshold();
    private static final long fragmentCompactionReadThreshold = ConfigDescriptor.getInstance().getConfig().getFragmentCompactionReadThreshold();

    private List<List<FragmentMeta>> toCompactFragmentGroups;

    @Override
    public boolean needCompaction() throws Exception {
        //集中信息（初版主要是统计分区热度）
        Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> fragmentHeatPair = metaManager
                .loadFragmentHeat();
        Map<FragmentMeta, Long> fragmentHeatWriteMap = fragmentHeatPair.getK();
        Map<FragmentMeta, Long> fragmentHeatReadMap = fragmentHeatPair.getV();
        if (fragmentHeatWriteMap == null) {
            fragmentHeatWriteMap = new HashMap<>();
        }
        if (fragmentHeatReadMap == null) {
            fragmentHeatReadMap = new HashMap<>();
        }

        List<FragmentMeta> fragmentMetaSet = metaManager.getFragments();

        long totalHeats = 0;
        for (Map.Entry<FragmentMeta, Long> fragmentHeatReadEntry : fragmentHeatReadMap.entrySet()) {
            totalHeats += fragmentHeatReadEntry.getValue();
        }
        double averageReadHeats = totalHeats * 1.0 / fragmentHeatReadMap.size();

        List<FragmentMeta> candidateFragments = new ArrayList<>();
        // 判断是否要合并不再被写入的的历史分片
        for (FragmentMeta fragmentMeta : fragmentMetaSet) {
            long writeLoad = fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
            long readLoad = fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
            if (fragmentMeta.getTimeInterval().getEndTime() != Long.MAX_VALUE && writeLoad < fragmentCompactionWriteThreshold && readLoad < averageReadHeats * fragmentCompactionReadRatioThreshold && readLoad > fragmentCompactionReadThreshold) {
                candidateFragments.add(fragmentMeta);
            }
        }

        toCompactFragmentGroups = packFragmentsByGroup(candidateFragments);

        return !toCompactFragmentGroups.isEmpty();
    }

    @Override
    public void compact() throws Exception {
        logger.info("start to compact low write fragments");
        Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();
        for (List<FragmentMeta> fragmentGroup : toCompactFragmentGroups) {
            // 分别计算每个du的数据量，取其中数据量最多的du作为目标合并du
            StorageUnitMeta maxStorageUnitMeta = fragmentGroup.get(0).getMasterStorageUnit();
            long maxStorageUnitPoint = 0;
            Map<String, Long> storageUnitPointsMap = new HashMap<>();
            long totalPoints = 0;
            for (FragmentMeta fragmentMeta : fragmentGroup) {
                long pointsNum = storageUnitPointsMap.getOrDefault(fragmentMeta.getMasterStorageUnitId(), 0L);
                pointsNum += fragmentMetaPointsMap.getOrDefault(fragmentMeta, 0L);
                if (pointsNum > maxStorageUnitPoint) {
                    maxStorageUnitMeta = fragmentMeta.getMasterStorageUnit();
                }
                storageUnitPointsMap.put(fragmentMeta.getMasterStorageUnitId(), pointsNum);
                totalPoints += fragmentMetaPointsMap.getOrDefault(fragmentMeta, 0L);
            }

            compactFragmentGroupToTargetStorageUnit(fragmentGroup, maxStorageUnitMeta, totalPoints);
        }
    }
}
