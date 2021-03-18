package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class MaxValueQueryPlan extends AggregateQueryPlan {

	protected MaxValueQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.MAX_VALUE);
	}

	protected MaxValueQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
