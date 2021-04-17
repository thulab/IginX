package cn.edu.tsinghua.iginx.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.AVG;

public class AvgQueryPlan extends AggregateQueryPlan {

	private static final Logger logger = LoggerFactory.getLogger(AvgQueryPlan.class);

	public AvgQueryPlan(List<String> paths, long startTime, long endTime, long storageEngineId, String storageUnitId) {
		super(paths, startTime, endTime, storageEngineId, storageUnitId);
		this.setIginxPlanType(AVG);
	}

	public AvgQueryPlan(List<String> paths, long startTime, long endTime) {
		this(paths, startTime, endTime, -1L, "");
	}
}
