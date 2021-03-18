package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class CountQueryPlan extends AggregateQueryPlan {

	protected CountQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.COUNT);
	}

	protected CountQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
