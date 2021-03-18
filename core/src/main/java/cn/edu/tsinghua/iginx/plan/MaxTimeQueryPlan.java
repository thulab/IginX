package cn.edu.tsinghua.iginx.plan;

import java.util.List;

public class MaxTimeQueryPlan extends AggregateQueryPlan {

	protected MaxTimeQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.MAX_TIME);
	}

	protected MaxTimeQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
