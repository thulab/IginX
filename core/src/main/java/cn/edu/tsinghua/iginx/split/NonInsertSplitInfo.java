package cn.edu.tsinghua.iginx.split;

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.plan.IginxPlan;

public class NonInsertSplitInfo extends SplitInfo {

    private TimeInterval timeInterval;

    private TimeSeriesInterval timeSeriesInterval;

    public NonInsertSplitInfo(TimeInterval timeInterval, TimeSeriesInterval timeSeriesInterval, StorageUnitMeta storageUnit) {
        super(storageUnit);
        this.timeInterval = timeInterval;
        this.timeSeriesInterval = timeSeriesInterval;
    }

    public NonInsertSplitInfo(TimeInterval timeInterval, TimeSeriesInterval timeSeriesInterval, StorageUnitMeta storageUnit,
                     IginxPlan.IginxPlanType type, int combineGroup) {
        super(storageUnit, type, combineGroup);
        this.timeInterval = timeInterval;
        this.timeSeriesInterval = timeSeriesInterval;
    }

    public TimeInterval getTimeInterval() {
        return timeInterval;
    }

    public void setTimeInterval(TimeInterval timeInterval) {
        this.timeInterval = timeInterval;
    }

    public TimeSeriesInterval getTimeSeriesInterval() {
        return timeSeriesInterval;
    }

    public void setTimeSeriesInterval(TimeSeriesInterval timeSeriesInterval) {
        this.timeSeriesInterval = timeSeriesInterval;
    }
}
