package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class MinTimeQueryPlan extends AggregateQueryPlan {

	protected MinTimeQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.MIN_TIME);
	}

	protected MinTimeQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
