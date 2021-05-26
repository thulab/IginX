package cn.edu.tsinghua.iginx.plan;

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.MIN;

public class MinQueryPlan extends AggregateQueryPlan {

    private static final Logger logger = LoggerFactory.getLogger(MinQueryPlan.class);

    public MinQueryPlan(List<String> paths, long startTime, long endTime, StorageUnitMeta storageUnit) {
        super(paths, startTime, endTime, storageUnit);
        this.setIginxPlanType(MIN);
    }

    public MinQueryPlan(List<String> paths, long startTime, long endTime) {
        this(paths, startTime, endTime, null);
    }
}
