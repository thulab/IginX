package cn.edu.tsinghua.iginx.compaction;

import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Migration;
import cn.edu.tsinghua.iginx.engine.shared.operator.ShowTimeSeries;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.GlobalSource;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;

import java.util.*;

public abstract class Compaction {
    protected final static PhysicalEngine physicalEngine = PhysicalEngineImpl.getInstance();
    protected final IMetaManager metaManager = DefaultMetaManager.getInstance();

    public abstract boolean needCompaction() throws Exception;

    public abstract void compact() throws Exception;

    protected List<List<FragmentMeta>> packFragmentsByGroup(List<FragmentMeta> fragmentMetas) {
        // 排序以减少算法时间复杂度
        fragmentMetas.sort((o1, o2) -> {
            // 先按照时间维度排序，再按照时间序列维度排序
            if (o1.getTimeInterval().getStartTime() == o2.getTimeInterval().getStartTime()) {
                return o1.getTsInterval().getStartTimeSeries().compareTo(o2.getTsInterval().getEndTimeSeries());
            } else {
                // 所有分片在时间维度上是统一的，因此只需要根据起始时间排序即可
                return Long.compare(o1.getTimeInterval().getStartTime(), o2.getTimeInterval().getStartTime());
            }
        });

        // 对筛选出来要合并的所有分片按连通性进行分组（同一组中的分片可以合并）
        List<List<FragmentMeta>> result = new ArrayList<>();
        List<FragmentMeta> lastFragmentGroup = new ArrayList<>();
        FragmentMeta lastFragment = null;
        for (FragmentMeta fragmentMeta : fragmentMetas) {
            if (lastFragment == null) {
                lastFragmentGroup.add(fragmentMeta);
            } else {
                if (isNext(lastFragment, fragmentMeta)) {
                    lastFragmentGroup.add(fragmentMeta);
                } else {
                    if (lastFragmentGroup.size() > 1) {
                        result.add(lastFragmentGroup);
                    }
                    lastFragmentGroup = new ArrayList<>();
                }
            }
            lastFragment = fragmentMeta;
        }
        return result;
    }

    private boolean isNext(FragmentMeta firstFragment, FragmentMeta secondFragment) {
        return firstFragment.getTimeInterval().equals(secondFragment.getTimeInterval()) && firstFragment.getTsInterval().getEndTimeSeries().equals(secondFragment.getTsInterval().getStartTimeSeries());
    }

    protected void compactFragmentGroupToTargetStorageUnit(List<FragmentMeta> fragmentGroup, StorageUnitMeta targetStorageUnit, long totalPoints) throws PhysicalException {
        String startTimeseries = fragmentGroup.get(0).getTsInterval().getStartTimeSeries();
        String endTimeseries = fragmentGroup.get(0).getTsInterval().getEndTimeSeries();
        long startTime = fragmentGroup.get(0).getTimeInterval().getStartTime();
        long endTime = fragmentGroup.get(0).getTimeInterval().getEndTime();

        for (FragmentMeta fragmentMeta : fragmentGroup) {
            String storageUnitId = fragmentMeta.getMasterStorageUnitId();
            if (!storageUnitId.equals(targetStorageUnit.getId())) {
                // 重写该分片的数据
                Set<String> pathRegexSet = new HashSet<>();
                ShowTimeSeries showTimeSeries = new ShowTimeSeries(new GlobalSource(), pathRegexSet, null,
                        Integer.MAX_VALUE, 0);
                RowStream rowStream = physicalEngine.execute(showTimeSeries);
                SortedSet<String> pathSet = new TreeSet<>();
                rowStream.getHeader().getFields().forEach(field -> {
                    String timeSeries = field.getName();
                    if (timeSeries.contains("{") && timeSeries.contains("}")) {
                        timeSeries = timeSeries.split("\\{")[0];
                    }
                    if (fragmentMeta.getTsInterval().isContain(timeSeries)) {
                        pathSet.add(timeSeries);
                    }
                });
                Migration migration = new Migration(new GlobalSource(), fragmentMeta, new ArrayList<>(pathSet), targetStorageUnit);
                physicalEngine.execute(migration);
                // 更新存储点数信息
                metaManager.updateFragmentPoints(fragmentMeta, totalPoints);
            }
        }
        // TODO add write lock
        // 创建新分片
        FragmentMeta newFragment = new FragmentMeta(startTimeseries, endTimeseries, startTime, endTime, targetStorageUnit);
        DefaultMetaManager.getInstance().addFragment(newFragment);

        for (FragmentMeta fragmentMeta : fragmentGroup) {
            String storageUnitId = fragmentMeta.getMasterStorageUnitId();
            if (!storageUnitId.equals(targetStorageUnit.getId())) {
                // 删除原分片元数据信息
                DefaultMetaManager.getInstance().removeFragment(fragmentMeta);
            }
        }
        // TODO release write lock

        for (FragmentMeta fragmentMeta : fragmentGroup) {
            String storageUnitId = fragmentMeta.getMasterStorageUnitId();
            if (!storageUnitId.equals(targetStorageUnit.getId())) {
                // 删除原分片节点数据
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
}