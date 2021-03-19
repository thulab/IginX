package cn.edu.tsinghua.iginx.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.FIRST;

public class FirstQueryPlan extends AggregateQueryPlan {

	private static final Logger logger = LoggerFactory.getLogger(FirstQueryPlan.class);

	public FirstQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(FIRST);
	}

	public FirstQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
