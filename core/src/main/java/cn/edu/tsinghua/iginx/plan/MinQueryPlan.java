package cn.edu.tsinghua.iginx.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.MIN;

public class MinQueryPlan extends AggregateQueryPlan {

	private static final Logger logger = LoggerFactory.getLogger(MinQueryPlan.class);

	public MinQueryPlan(List<String> paths, long startTime, long endTime) {
		super(paths, startTime, endTime);
		this.setIginxPlanType(MIN);
	}

	public MinQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		this(paths, startTime, endTime);
		this.setStorageEngineId(storageEngineId);
	}
}
