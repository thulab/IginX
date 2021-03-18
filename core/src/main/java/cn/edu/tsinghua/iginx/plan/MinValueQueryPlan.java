package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class MinValueQueryPlan extends AggregateQueryPlan {

	protected MinValueQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.MIN_VALUE);
	}

	protected MinValueQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
