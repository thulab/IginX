package cn.edu.tsinghua.iginx.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.LAST;

public class LastQueryPlan extends AggregateQueryPlan {

	private static final Logger logger = LoggerFactory.getLogger(LastQueryPlan.class);

	public LastQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(LAST);
	}

	public LastQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		super(paths, startTime, endTime, storageEngineId);
	}
}
