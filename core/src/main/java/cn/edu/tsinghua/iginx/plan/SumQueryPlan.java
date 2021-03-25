package cn.edu.tsinghua.iginx.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.SUM;

public class SumQueryPlan extends AggregateQueryPlan {

	private static final Logger logger = LoggerFactory.getLogger(SumQueryPlan.class);

	public SumQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(SUM);
	}

	public SumQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		this(paths, startTime, endTime);
		this.setStorageEngineId(storageEngineId);
	}
}
