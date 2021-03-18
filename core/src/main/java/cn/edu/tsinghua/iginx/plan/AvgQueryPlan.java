package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class AvgQueryPlan extends AggregateQueryPlan {

	protected AvgQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.AVG);
	}

	protected AvgQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
