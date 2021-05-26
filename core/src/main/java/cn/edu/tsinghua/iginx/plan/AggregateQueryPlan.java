package cn.edu.tsinghua.iginx.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.AGGREGATE_QUERY;

public abstract class AggregateQueryPlan extends DataPlan {

    private static final Logger logger = LoggerFactory.getLogger(AggregateQueryPlan.class);

    protected AggregateQueryPlan(List<String> paths, long startTime, long endTime) {
        super(true, paths, startTime, endTime);
        this.setIginxPlanType(AGGREGATE_QUERY);
        this.setSync(true);
    }
}
