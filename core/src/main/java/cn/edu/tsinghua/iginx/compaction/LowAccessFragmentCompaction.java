package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LowAccessFragmentCompaction extends Compaction {

    private static final Logger logger = LoggerFactory.getLogger(LowAccessFragmentCompaction.class);
    private static final long fragmentCompactionWriteThreshold = ConfigDescriptor.getInstance().getConfig().getFragmentCompactionWriteThreshold();
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

        List<FragmentMeta> candidateFragments = new ArrayList<>();
        // 判断是否要合并不再被写入的的历史分片
        for (FragmentMeta fragmentMeta : fragmentMetaSet) {
            long writeLoad = fragmentHeatWriteMap.getOrDefault(fragmentMeta, 0L);
            long readLoad = fragmentHeatReadMap.getOrDefault(fragmentMeta, 0L);
            if (fragmentMeta.getTimeInterval().getEndTime() != Long.MAX_VALUE && writeLoad < fragmentCompactionWriteThreshold && readLoad <= fragmentCompactionReadThreshold) {
                candidateFragments.add(fragmentMeta);
            }
        }

        toCompactFragmentGroups = packFragmentsByGroup(candidateFragments);

        return !toCompactFragmentGroups.isEmpty();
    }

    @Override
    public void compact() throws Exception {
        logger.info("start to compact low access fragments");
        Map<FragmentMeta, Long> fragmentMetaPointsMap = metaManager.loadFragmentPoints();

        // 优先存储到点数最少的节点上（剩余磁盘空间较大）
        Map<Long, Long> storageEnginePointsMap = new HashMap<>();
        for (Map.Entry<FragmentMeta, Long> fragmentMetaPointsEntry : fragmentMetaPointsMap.entrySet()) {
            FragmentMeta fragmentMeta = fragmentMetaPointsEntry.getKey();
            long storageEngineId = fragmentMeta.getMasterStorageUnit().getStorageEngineId();
            long points = fragmentMetaPointsEntry.getValue();
            long allPoints = storageEnginePointsMap.getOrDefault(storageEngineId, 0L);
            allPoints += points;
            storageEnginePointsMap.put(storageEngineId, allPoints);
        }
        long minPoints = Long.MAX_VALUE;
        long minStorageEngineId = 0;
        for (Map.Entry<Long, Long> storageEnginePointsEntry : storageEnginePointsMap.entrySet()) {
            if (minPoints > storageEnginePointsEntry.getValue()) {
                minStorageEngineId = storageEnginePointsEntry.getKey();
                minPoints = storageEnginePointsEntry.getValue();
            }
        }

        for (List<FragmentMeta> fragmentGroup : toCompactFragmentGroups) {
            // 分别计算每个du的数据量，取其中数据量最多的du作为目标合并du
            StorageUnitMeta maxStorageUnitMeta = fragmentGroup.get(0).getMasterStorageUnit();
            long maxStorageUnitPoint = 0;
            long totalPoints = 0;
            Map<String, Long> storageUnitPointsMap = new HashMap<>();
            for (FragmentMeta fragmentMeta : fragmentGroup) {
                // 优先按照节点当前存储的点数最小做选择
                if (fragmentMeta.getMasterStorageUnit().getStorageEngineId() == minStorageEngineId) {
                    long pointsNum = storageUnitPointsMap.getOrDefault(fragmentMeta.getMasterStorageUnitId(), 0L);
                    pointsNum += fragmentMetaPointsMap.getOrDefault(fragmentMeta, 0L);
                    if (pointsNum > maxStorageUnitPoint) {
                        maxStorageUnitMeta = fragmentMeta.getMasterStorageUnit();
                    }
                    storageUnitPointsMap.put(fragmentMeta.getMasterStorageUnitId(), pointsNum);
                }
                totalPoints += fragmentMetaPointsMap.getOrDefault(fragmentMeta, 0L);
            }

            compactFragmentGroupToTargetStorageUnit(fragmentGroup, maxStorageUnitMeta, totalPoints);
        }
    }
}
