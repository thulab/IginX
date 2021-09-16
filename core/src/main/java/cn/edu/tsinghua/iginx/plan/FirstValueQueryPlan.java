package cn.edu.tsinghua.iginx.plan;

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.FIRST_VALUE;

public class FirstValueQueryPlan extends AggregateQueryPlan {

    private static final Logger logger = LoggerFactory.getLogger(FirstValueQueryPlan.class);

    public FirstValueQueryPlan(List<String> paths, long startTime, long endTime, StorageUnitMeta storageUnit) {
        super(paths, startTime, endTime, storageUnit);
        this.setIginxPlanType(FIRST_VALUE);
    }

    public FirstValueQueryPlan(List<String> paths, long startTime, long endTime) {
        this(paths, startTime, endTime, null);
    }

}
