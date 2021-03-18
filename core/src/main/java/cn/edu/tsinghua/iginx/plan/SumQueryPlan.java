package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class SumQueryPlan extends AggregateQueryPlan {

	protected SumQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.SUM);
	}

	protected SumQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
