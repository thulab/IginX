package cn.edu.tsinghua.iginx.metadata.utils;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesRange;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.*;

public class FragmentUtils {

    public static Pair<Map<TimeInterval, List<FragmentMeta>>, List<FragmentMeta>> keyFromTSIntervalToTimeInterval(Map<TimeSeriesRange,
        List<FragmentMeta>> fragmentMapByTSInterval) {
        Map<TimeInterval, List<FragmentMeta>> fragmentMapByTimeInterval = new HashMap<>();
        List<FragmentMeta> dummyFragments = new ArrayList<>();
        fragmentMapByTSInterval.forEach((k, v) -> {
            v.forEach(fragmentMeta -> {
                if (fragmentMeta.isDummyFragment()) {
                    dummyFragments.add(fragmentMeta);
                    return;
                }
                if (fragmentMapByTimeInterval.containsKey(fragmentMeta.getTimeInterval())) {
                    fragmentMapByTimeInterval.get(fragmentMeta.getTimeInterval()).add(fragmentMeta);
                } else {
                    fragmentMapByTimeInterval.put(
                        fragmentMeta.getTimeInterval(),
                        new ArrayList<>(Collections.singletonList(fragmentMeta))
                    );
                }
            });
        });
        return new Pair<>(fragmentMapByTimeInterval, dummyFragments);
    }
}