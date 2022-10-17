package cn.edu.tsinghua.iginx.metadata.utils;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;

import java.util.*;

public class FragmentUtils {

    public static Map<TimeInterval, List<FragmentMeta>> keyFromTSIntervalToTimeInterval(Map<TimeSeriesInterval,
        List<FragmentMeta>> fragmentMapByTSInterval) {
        Map<TimeInterval, List<FragmentMeta>> fragmentMapByTimeInterval = new HashMap<>();
        fragmentMapByTSInterval.forEach((k, v) -> {
            v.forEach(fragmentMeta -> {
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
        return fragmentMapByTimeInterval;
    }
}