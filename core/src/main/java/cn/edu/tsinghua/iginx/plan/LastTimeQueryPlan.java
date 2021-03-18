package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class LastTimeQueryPlan extends AggregateQueryPlan {

	protected LastTimeQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.LAST_TIME);
	}

	protected LastTimeQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
