package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class LastValueQueryPlan extends AggregateQueryPlan {

	protected LastValueQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.LAST_VALUE);
	}

	protected LastValueQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
