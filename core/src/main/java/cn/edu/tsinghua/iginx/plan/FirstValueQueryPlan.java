package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class FirstValueQueryPlan extends AggregateQueryPlan {

	protected FirstValueQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.FIRST_VALUE);
	}

	protected FirstValueQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
