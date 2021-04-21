package cn.edu.tsinghua.iginx.plan;

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.LAST;

public class LastQueryPlan extends AggregateQueryPlan {

	private static final Logger logger = LoggerFactory.getLogger(LastQueryPlan.class);

	public LastQueryPlan(List<String> paths, long startTime, long endTime, StorageUnitMeta storageUnit) {
		super(paths, startTime, endTime, storageUnit);
		this.setIginxPlanType(LAST);
	}

	public LastQueryPlan(List<String> paths, long startTime, long endTime) {
		this(paths, startTime, endTime, null);
	}
}
