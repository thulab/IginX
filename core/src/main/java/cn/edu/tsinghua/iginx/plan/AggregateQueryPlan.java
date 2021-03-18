package cn.edu.tsinghua.iginx.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AggregateQueryPlan extends DataPlan {

	private static final Logger logger = LoggerFactory.getLogger(AggregateQueryPlan.class);

	protected AggregateQueryPlan(List<String> paths, long startTime, long endTime) {
		super(true, paths, startTime, endTime);
		this.setIginxPlanType(IginxPlanType.AGGREGATE_QUERY);
	}

	protected AggregateQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId) {
		this(paths, startTime, endTime);
		this.setStorageEngineId(storageEngineId);
	}
}
