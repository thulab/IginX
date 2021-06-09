package cn.edu.tsinghua.iginx.plan;

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.SUM;

public class SumQueryPlan extends AggregateQueryPlan {

    private static final Logger logger = LoggerFactory.getLogger(SumQueryPlan.class);

    public SumQueryPlan(List<String> paths, long startTime, long endTime, StorageUnitMeta storageUnit) {
        super(paths, startTime, endTime, storageUnit);
        this.setIginxPlanType(SUM);
    }

    public SumQueryPlan(List<String> paths, long startTime, long endTime) {
        this(paths, startTime, endTime, null);
    }

}
