package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class FirstTimeQueryPlan extends AggregateQueryPlan {

	protected FirstTimeQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.FIRST_TIME);
	}

	protected FirstTimeQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
