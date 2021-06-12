package cn.edu.tsinghua.iginx.split;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;

public class InsertSplitInfo extends SplitInfo {

    private FragmentMeta fragment;

    public InsertSplitInfo(FragmentMeta fragment, StorageUnitMeta storageUnit) {
        super(storageUnit);
        this.fragment = fragment;
    }

    public TimeSeriesInterval getIdealTsInterval() {
        return fragment.getIdealTsInterval();
    }

    public TimeInterval getIdealTimeInterval() {
        return fragment.getIdealTimeInterval();
    }

    public TimeSeriesInterval getActualTsInterval() {
        return fragment.getActualTsInterval();
    }

    public TimeInterval getActualTimeInterval() {
        return fragment.getActualTimeInterval();
    }
}
