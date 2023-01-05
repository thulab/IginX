package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FragmentDeletionCompaction extends Compaction {

    private static final Logger logger = LoggerFactory.getLogger(FragmentDeletionCompaction.class);
    private List<FragmentMeta> toDeletionFragments;

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

        long totalHeats = 0;
        for (Map.Entry<FragmentMeta, Long> fragmentHeatReadEntry : fragmentHeatReadMap.entrySet()) {
            totalHeats += fragmentHeatReadEntry.getValue();
        }
        double limitReadHeats = totalHeats * 1.0 / fragmentHeatReadMap.size();

        // 判断是否要删除可定制化副本生成的冗余分片
        // TODO

        return !toDeletionFragments.isEmpty();
    }

    @Override
    public void compact() throws PhysicalException {
        for (FragmentMeta fragmentMeta : toDeletionFragments) {
            // 删除可定制化副本分片元数据
            // TODO

            // 删除节点数据
            List<String> paths = new ArrayList<>();
            paths.add(fragmentMeta.getMasterStorageUnitId() + "*");
            List<TimeRange> timeRanges = new ArrayList<>();
            timeRanges.add(new TimeRange(fragmentMeta.getTimeInterval().getStartTime(), true,
                    fragmentMeta.getTimeInterval().getEndTime(), false));
            Delete delete = new Delete(new FragmentSource(fragmentMeta), timeRanges, paths, null);
            physicalEngine.execute(delete);
        }
    }
}
