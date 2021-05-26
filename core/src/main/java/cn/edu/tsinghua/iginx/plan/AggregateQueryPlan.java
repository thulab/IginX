package cn.edu.tsinghua.iginx.plan;

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.AGGREGATE_QUERY;

public abstract class AggregateQueryPlan extends DataPlan {

    private static final Logger logger = LoggerFactory.getLogger(AggregateQueryPlan.class);

    protected AggregateQueryPlan(List<String> paths, long startTime, long endTime, StorageUnitMeta storageUnit) {
        super(true, paths, startTime, endTime, storageUnit);
        this.setIginxPlanType(AGGREGATE_QUERY);
        this.setSync(true);
    }

    protected AggregateQueryPlan(List<String> paths, long startTime, long endTime) {
        this(paths, startTime, endTime, null);
    }
}
